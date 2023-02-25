
//ThisBuild / version := "0.1.0-SNAPSHOT"
//
//ThisBuild / scalaVersion := "2.11.10"
name := "spark-streaming-poclab"
version := "1.0"

//scalaVersion := "2.11.10"
scalaVersion := "2.12.10"

//lazy val root = (project in file("."))
//  .settings(
//    name := "spark-streaming-poclab"
//  )

//val sparkVersion = "2.4.1"
val sparkVersion = "3.2.2"
val kafkaVersion = "2.4.0"
val log4jVersion = "2.17.0"

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0.1"
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-pool2" % "2.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  // streaming-kafka
//  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion
)
//externalResolvers := Seq("central repository".at("https://repo1.maven.org/maven2/"))

scalacOptions ++= Seq("-deprecation", "-feature")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
