name := "KafkaStream"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.2.1","org.apache.spark" % "spark-streaming_2.10" % "1.2.1","org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.1","org.apache.kafka" % "kafka_2.10" % "0.8.1.1")
    