name         := "db4bd_2021"
scalaVersion := "2.13.6"

libraryDependencies ++= Seq(
  "org.rocksdb"              % "rocksdbjni"                           % "6.6.4",
  "com.datastax.oss"         % "java-driver-core"                     % "4.13.0",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.14.2",
  "org.apache.kafka"         % "kafka-streams"                        % "2.8.0",
  "org.apache.kafka"        %% "kafka-streams-scala"                  % "2.8.0",
  "org.apache.kafka"         % "kafka-clients"                        % "2.8.0",
  "com.sksamuel.avro4s"     %% "avro4s-core"                          % "4.0.9",
  "com.sksamuel.avro4s"     %% "avro4s-kafka"                         % "4.0.9",
  "org.http4s"              %% "http4s-blaze-server"                  % "0.21.22",
  "org.http4s"              %% "http4s-blaze-client"                  % "0.21.22",
  "com.lihaoyi"             %% "fastparse"                            % "2.2.2",
  "ch.qos.logback"           % "logback-core"                         % "1.2.3",
  "ch.qos.logback"           % "logback-classic"                      % "1.2.3",
  "com.sparkjava"            % "spark-core"                           % "2.9.3",
  "com.squareup.okhttp3"     % "okhttp"                               % "4.9.0",
  "org.scalatest"           %% "scalatest"                            % "3.2.9" % Test,
  "org.apache.kafka"         % "kafka-streams-test-utils"             % "2.8.0" % Test
)
