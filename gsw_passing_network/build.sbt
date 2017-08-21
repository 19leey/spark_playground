name := "GSW Passing Network"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"
  
