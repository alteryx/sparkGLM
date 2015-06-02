import sbt._
import Process._
import Keys._
import sbtassembly._
import AssemblyKeys._

name := "sparkGLM"

version := "0.1"

organization := "Alteryx"

scalaVersion := "2.10.4"

spName := "Alteryx/sparkGLM"

sparkVersion := "1.4.0-SNAPSHOT"

sparkComponents ++= Seq("sql", "mllib")

resolvers += Resolver.mavenLocal

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
