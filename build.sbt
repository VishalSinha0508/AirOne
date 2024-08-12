ThisBuild / version := "0.1.0-SNAPSHOT"

name := "AirOne"

ThisBuild / scalaVersion := "2.12.18"

val versions = new {
  val spark = "3.5.1"
}


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" ,
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "io.delta" %% "delta-spark" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.11" % Test,
  "com.microsoft.sqlserver" % "mssql-jdbc" % "9.2.1.jre8"

)
lazy val root = (project in file("."))
  .settings(
    name := "AirOne"
  )
