import sbt._
import Process._
import Keys._

lazy val commonSettings = Seq(
  organization := "com.Alteryx",
  scalaVersion := "2.10.4",
  sparkVersion := "1.4.0",
  licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "sparkGLM",
    spName := "Alteryx/" + name.value,
    version := "0.0.1",
    sparkComponents ++= Seq("sql", "mllib"),
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.2",
      "org.slf4j" % "slf4j-log4j12" % "1.7.2",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.scalanlp" % "breeze_2.10" % "0.11.2",
      "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
      "edu.berkeley.cs.amplab" % "mlmatrix" % "0.1" from "https://s3-us-west-1.amazonaws.com/amp-ml-matrix/2.10/mlmatrix_2.10-0.1.jar"
    ),
    resolvers ++= Seq(
      "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
      "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Spray" at "http://repo.spray.cc",
      "Amplab ml-matrix repo" at "https://s3-us-west-1.amazonaws.com/",
      Resolver.mavenLocal
    )
  )

assemblyJarName in assembly := name.value + "-assembly-" + version.value + ".jar"

assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*)              => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html"      => MergeStrategy.first
    case "application.conf"                                 => MergeStrategy.concat
    case "reference.conf"                                   => MergeStrategy.concat
    case "log4j.properties"                                 => MergeStrategy.first
    case m if m.toLowerCase.endsWith("manifest.mf")         => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf/services.*$") => MergeStrategy.concat
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")     => MergeStrategy.discard
    case _ => MergeStrategy.first
}
