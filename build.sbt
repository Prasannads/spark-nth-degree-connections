import sbt.{Credentials, Path}

name := "nth-degree-connections-spark"
organization := "com.soundcloud.data"
version := "1.0"
scalaVersion := "2.12.10"
credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
resolvers += Resolver.mavenLocal
inThisBuild(List(assemblyJarName in assembly := "calculate-nth-degree-connections.jar"))
libraryDependencies ++= {
  sys.props += "packaging.type" -> "jar"
  Seq(
    "org.scalatest"              %% "scalatest"     % "3.1.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    "org.apache.spark"           %% "spark-sql"     % "2.4.5",
    "org.apache.hadoop"           % "hadoop-hdfs"   % "3.2.1",
    "org.apache.hadoop"           % "hadoop-common" % "3.2.1",
    "org.apache.hadoop"           % "hadoop-client" % "3.2.1"
  )
}

fork in run := true
assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*)                                       => MergeStrategy.discard
  case x                                                                   => MergeStrategy.first
}
