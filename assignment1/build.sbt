name := "assignment1"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"