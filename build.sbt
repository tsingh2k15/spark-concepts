name := """apache-spark-2-scala-starter-template"""

version := "1.0"

scalaVersion := "2.11.12"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0"
)

javaOptions in(Test, run) ++= Seq("-Dspark.master=local",
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")

outputStrategy := Some(StdoutOutput)

fork := true

coverageEnabled in Test := true