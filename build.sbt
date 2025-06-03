name := "custom-blp-datasource"

version := "0.1"

scalaVersion := "2.12.15"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"
)

