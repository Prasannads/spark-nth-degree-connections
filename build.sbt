lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.soundcloud.data.nthdegreeconnections",
      version := "1.0.0",
      scalaVersion := "2.12.10",
      assemblyJarName in assembly := "calculate-nth-degree-connections.jar"
    )
  ),
  name := "myservice",
  libraryDependencies ++= List(
    "org.scalatest"              %% "scalatest"     % "3.1.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    "org.apache.spark"           %% "spark-sql"     % "2.4.5"
  )
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}
