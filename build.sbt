name := "FirstSparkStreaming"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-streaming_2.10" % "1.2.1",
    "org.apache.spark" % "spark-core" % "1.2.1"
)