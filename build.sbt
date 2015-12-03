name := "simple-example"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.1"%"provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.1" %"provided"

libraryDependencies += ("org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.1").exclude("org.spark-project.spark","unused")
