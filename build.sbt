
name := "spark_dataset_analyzer"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "com.databricks" %% "spark-csv" % "1.5.0"
  )
}

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"

assemblyJarName in assembly := "analyzer.jar"
test in assembly := {}
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

addCommandAlias("build", ";clean;assembly")
