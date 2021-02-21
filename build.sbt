name := "big-data-take-home-assignment"

organization := "co.thebeat"

version := "1.0"

scalaVersion := "2.11.12"

val versions = new {
  val sparkVersion = "2.4.3"
  val jts = "1.15.1"
  val scalaTestVersion = "3.0.5"
  val scalameter = "0.18"
  val json4s = "3.2.10"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.sparkVersion,
  "org.apache.spark" %% "spark-sql" % versions.sparkVersion,
  "org.apache.spark" %% "spark-hive" % versions.sparkVersion,
  "org.locationtech.jts" % "jts-core" % versions.jts,
  "org.json4s" %% "json4s-native" % versions.json4s,
  "org.scalatest" %% "scalatest" % versions.scalaTestVersion % Test,
  "com.storm-enroute" %% "scalameter" % versions.scalameter % Test
)
