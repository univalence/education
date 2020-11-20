package actor4fun.internal

import actor4fun.internal.RemoteActorSystem.serialize
import actor4fun.internal.network.{ActorEndPointGrpc, NetActorMessage, NetActorRef}
import actor4fun.{ActorMessage, ActorRef, ActorSystem}
import com.google.protobuf.ByteString
import java.net.URI
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Try

case class RemoteActorRef(
                           override val name: String,
                           actorSystemRef: RemoteActorSystemRef,
                           actorSystem: RemoteActorSystem
                         ) extends ActorRef {

  val logger: Logger = LoggerFactory.getLogger(s"$name-ref")

  /** @inheritdoc */
  override def sendFrom(sender: ActorRef, message: Any): Unit = {
    logger.debug(s"sending $message to $sender")
    this match {
      case RemoteActorRef(name, actorSystemRef, system)
        if actorSystemRef == system.self =>
        system
          .actors(system.refs(name))
          .pushMessage(ActorMessage(sender, message))

      case RemoteActorRef(_, actorSystemRef, system)
        if actorSystemRef != system.self =>
        sender match {
          case RemoteActorRef(_, senderSysRef, system) =>
            val channel =
              system.connect(actorSystemRef.host, actorSystemRef.port)
            try {
              val senderRef = NetActorRef(
                senderSysRef.host,
                senderSysRef.port,
                sender.name
              )
              val receiverRef = NetActorRef(
                actorSystemRef.host,
                actorSystemRef.port,
                this.name
              )
              val payload = ByteString.copyFrom(serialize(message))

              ActorEndPointGrpc
                .blockingStub(channel)
                .receive(
                  NetActorMessage(
                    sender = Option(senderRef),
                    receiver = Option(receiverRef),
                    payload = payload
                  )
                )
            } finally { channel.shutdown() }

          // TODO what if sender is local ref?
//          case LocalActorRef(_, _) => ???

          case _ =>
            throw new IllegalArgumentException(s"unmanaged sender reference $sender")
        }

      case _ =>
        throw new IllegalCallerException(s"unmanaged sender reference $this")
    }
  }
}
object RemoteActorRef {
  val uriActorScheme = "actor"

  /**
   * Try to get actor name and actor system reference from a URI.
   *
   * @param uriStr string representing a URI
   * @return actor name and actor system reference
   */
  private[internal] def fromURI(
                              uriStr: String
                            ): Option[(String, RemoteActorSystemRef)] = {
    for {
      uri    <- Try { URI.create(uriStr) }.toOption
      scheme <- Option(uri.getScheme)
      if scheme == uriActorScheme
      host <- Option(uri.getHost)
      port <- Option(uri.getPort).filter(_ >= 0)
      path <- Option(uri.getPath)
    } yield {
      val name = if (path.startsWith("/")) path.tail else path

      (name, RemoteActorSystemRef(host, port))
    }
  }

  def fromURI(uriStr: String, system: ActorSystem): Option[RemoteActorRef] =
    system match {
      case _: LocalActorSystem => None
      case remoteSys: RemoteActorSystem =>
        for ((name, sysRef) <- fromURI(uriStr))
          yield RemoteActorRef(name, sysRef, remoteSys)
    }
}