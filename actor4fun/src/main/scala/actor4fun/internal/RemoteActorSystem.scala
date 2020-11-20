package actor4fun.internal

import actor4fun.internal.network.ActorEndPointGrpc.ActorEndPoint
import actor4fun.internal.network._
import actor4fun.{Actor, ActorProperties, ActorRef, ActorSystemProperties}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.InetAddress
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** Registry for actors with ability to manage distant comminucations.
  *
  * It manages directly actors it creates and establishes link with
  * remote actor system when actors needs to communicate with remote
  * ones.
  *
  * @param name actor system name
  * @param host host name or adress of this actor system
  * @param port listening port for incoming communication
  * @param properties actor system properties
  * @param ec thread pool used by the actor system
  */
class RemoteActorSystem private[actor4fun] (
    override val name: String,
    val host: String,
    val port: Int,
    override val properties: ActorSystemProperties
)(implicit
    ec: ExecutionContext
) extends BaseActorSystem(name, properties, ec) { system =>

  /** IP adress of this actor system. */
  val address: String = InetAddress.getByName(host).getHostAddress

  val self: RemoteActorSystemRef = RemoteActorSystemRef(address, port)
  private val server: Server     = createServer(port)

  locally {
    logger.debug(s"starting server on $port...")
    server.start()
    logger.info(s"server started on $port")
  }

  private def createServer(port: Int): Server = {
    val service =
      ActorEndPointGrpc.bindService(
        new RemoteHandlerService,
        ExecutionContext.global
      )
    val server = ServerBuilder.forPort(port)
    server.addService(service)

    server.build()
  }

  /** @inheritdoc */
  override def registerAndManage(
      name: String,
      actor: Actor,
      actorProperties: ActorProperties
  ): ActorRef =
    if (refs.contains(name)) {
      logger.error(s"$name already registered")
      throw new IllegalArgumentException(s"$name already registered")
    } else {
      val actorThread: ActorThread =
        new ActorThread(name, actor, actorProperties)
      val remoteRef: ActorRef = RemoteActorRef(name, self, system)

      actors.update(remoteRef, actorThread)
      refs.update(remoteRef.name, remoteRef)
      threads.update(
        remoteRef,
        Future { actorThread.start(remoteRef); actorThread }
      )

      logger.info(s"registered $remoteRef")

      remoteRef
    }

  /** Retrieve an actor reference by its name.
    *
    * Name might be of the form:
    *   - `<actor_name>` for actor local to this actor system
    *   - `actor://<host>:<port>/<actor_name>` to find a
    *     remote actor
    *
    * @param name actor to retrieve.
    * @return the actor reference or None if the name is unknown.
    */
  override def findActorForName(name: String): Option[ActorRef] = {
    logger.debug(s"finding $name")
    if (name.startsWith(s"${RemoteActorRef.uriActorScheme}://")) {
      val ref: Option[(String, RemoteActorSystemRef)] =
        RemoteActorRef.fromURI(name)

      val local: Option[ActorRef] =
        ref
          .filter { case (_, sysRef) => sysRef == self }
          .flatMap { case (name, _) => refs.get(name) }

      local.orElse {
        ref.flatMap { case (name, sysRef) =>
          val names: Set[String] = getRemoteNames(sysRef.host, sysRef.port)
          logger.debug(s"retrieve from $sysRef: $names")
          if (names.contains(name))
            Some(RemoteActorRef(name, sysRef, system))
          else
            None
        }
      }
    } else {
      refs.get(name)
    }
  }

  /** Get the names registered on a remote actor system.
    *
    * @param host remote actor system host.
    * @param port remote actor system port.
    * @return set of registered actor names on the remote actor system.
    */
  private def getRemoteNames(host: String, port: Int): Set[String] = {
    val channel: ManagedChannel = connect(host, port)
    try {
      ActorEndPointGrpc
        .blockingStub(channel)
        .actorNames(Empty())
        .names
        .toSet
    } finally { channel.shutdown() }
  }

  private[internal] def connect(
      host: String,
      port: Int
  ): ManagedChannel = {
    val builder = ManagedChannelBuilder.forAddress(host, port)
    builder.usePlaintext()

    builder.build()
  }

  /** @inheritdoc */
  override def shutdown(): Unit = {
    timer.cancel()
    server.shutdownNow()
    val future = Future.sequence(threads.values.toList)
    actors.values.foreach(_.shutdown())
    Await.ready(
      future,
      Duration(properties.shutdownTimeout._1, properties.shutdownTimeout._2)
    )
    logger.info(s"actor system has shutdown")
  }

  /** @inheritdoc */
  override def awaitTermination(): Unit = {
    val future = Future.sequence(threads.values.toList)

    server.awaitTermination()
    Await.ready(future, Duration.Inf)
  }

  /** Service to handle remote actor system requests.
    */
  class RemoteHandlerService extends ActorEndPoint {

    def toRemoteActorRef(netRef: NetActorRef): RemoteActorRef =
      RemoteActorRef(
        netRef.name,
        RemoteActorSystemRef(
          host = InetAddress.getByName(netRef.host).getHostAddress,
          port = netRef.port
        ),
        system
      )

    override def receive(request: NetActorMessage): Future[Ack] = {
      logger.debug(s"received $request")

      val response: Option[Ack] =
        for {
          receiver <- request.receiver.map(toRemoteActorRef)
          sender   <- request.sender.map(toRemoteActorRef)
        } yield sendRequest(receiver, sender, request)

      response.fold {
        val errorMessage =
          s"$name(${self.host}:${self.port}): receiver or sender is missing"
        logger.error(errorMessage)

        // TODO find better way to report this error
        Future.successful(Ack(isOk = false, error = errorMessage))
      } { r =>
        logger.debug(s"response: $r")

        Future.successful(r)
      }
    }

    private def sendRequest(
        receiver: RemoteActorRef,
        sender: RemoteActorRef,
        request: NetActorMessage
    ): Ack =
      if (receiver.actorSystemRef != self) {
        // receiver actor system is not the current one
        val errorMessage =
          s"$name(${self.host}:${self.port}): this system is not the target of ${receiver.actorSystemRef.host}:${receiver.actorSystemRef.port}"
        logger.error(errorMessage)

        Ack(isOk = false, error = errorMessage)
      } else if (!refs.contains(receiver.name)) {
        // this actor systems does not know the receiver actor name
        val errorMessage =
          s"$name(${self.host}:${self.port}): unknown actor with name ${receiver.name}"
        logger.error(errorMessage)

        Ack(isOk = false, error = errorMessage)
      } else {
        // TODO check for errors (eg. serialization, sending error...)
        val payload = deserializePayload(request.payload)
        val target  = refs(receiver.name)
        target.sendFrom(sender, payload)

        Ack(isOk = true)
      }

    private def deserializePayload(payload: ByteString): Any = {
      val data = Array.ofDim[Byte](payload.size())
      payload.copyTo(data, 0)

      RemoteActorSystem.deserialize(data)
    }

    override def actorNames(request: Empty): Future[ActorNames] = {
      logger.debug(s"asked for actor names")

      Future {
        ActorNames(refs.keys.toSeq)
      }
    }

  }

}

object RemoteActorSystem {

  def serialize(payload: Any): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    try {
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(payload)

      bos.toByteArray
    } finally {
      bos.close()
    }
  }

  def deserialize(payload: Array[Byte]): Any = {
    val bis = new ByteArrayInputStream(payload)
    try {
      val ois = new ObjectInputStream(bis)

      ois.readObject()
    } finally { bis.close() }
  }

}
