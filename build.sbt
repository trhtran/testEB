name := "EarlyBirds"
version := "1.0"
scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  ,"org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  )

// META-INF discarding
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

