name := "actor4fun"

version := "0.1"

scalaVersion := "2.13.4"

libraryDependencies ++= Seq(
  "ch.qos.logback"        % "logback-classic"      % "1.2.3",
  "ch.qos.logback"        % "logback-core"         % "1.2.3",
  "org.slf4j"             % "slf4j-api"            % "1.7.30",
  "io.grpc"               % "grpc-netty"           % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime"      % scalapb.compiler.Version.scalapbVersion % "protobuf",
  "org.scalatest"        %% "scalatest"            % "3.2.2"                                 % Test,
  "org.scalacheck"       %% "scalacheck"           % "1.14.1"                                % Test,
  "org.scalatestplus"    %% "scalacheck-1-14"      % "3.2.2.0"                               % Test
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
)
