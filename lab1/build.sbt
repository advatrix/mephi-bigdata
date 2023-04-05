ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val root = (project in file("."))
  .settings(
    name := "lab1",
    libraryDependencies ++= Seq(
      //"org.apache.hadoop" % "hadoop-core" % "2.10.2",
      "org.apache.hadoop" % "hadoop-client" % "2.10.2",
      "eu.bitwalker" % "UserAgentUtils" % "1.20",
      //"junit" % "junit" % "4.12" % Test,
      "org.apache.mrunit" % "mrunit" % "1.1.0" % Test classifier "hadoop2" ,
      "org.projectlombok" % "lombok" % "1.18.2",

    )
  )
