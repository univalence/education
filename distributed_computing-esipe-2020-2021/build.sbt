import sbt.Keys.libraryDependencies

name := "distributed_computing-esipe-2020-2021"

version := "0.1"

lazy val root =
  (project in file("."))
    .aggregate(mini_spark, spark_xp)

lazy val experiment =
  (project in file("experiment"))
    .settings(
      name := "experiment",
      scalaVersion := "2.13.4",
      libraryDependencies ++= Seq(
        "io.univalence" %% "actor4fun" % "0.0.2"
      )
    )

lazy val mini_spark =
  (project in file("mini_spark"))
    .settings(
      name := "mini_spark",
      scalaVersion := "2.13.4",
      libraryDependencies ++= Seq(
        "io.univalence" %% "actor4fun" % "0.0.2"
      )
    )

lazy val spark_xp =
  (project in file("spark_xp"))
    .settings(
      name := "spark_xp",
      scalaVersion := "2.12.12",
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "2.4.7",
        "org.apache.spark" %% "spark-sql"  % "2.4.7"
      )
    )
