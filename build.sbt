name := "spk-airplane"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0" % "provided"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0"