name := "bigdata-db"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.6.0",
  "org.slf4j"        % "slf4j-simple"  % "1.7.5",
  "com.fasterxml.jackson.core" % "jackson-databind"    % "2.11.2",
//  "org.apache.spark" %% "spark-sql" % "3.0.0"
  "org.scalatest" %% "scalatest" % "3.2.0" % Test
)
