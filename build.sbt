//import scala.scalanative.build.*

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "be.adamv"

ThisBuild / scalaVersion := "3.4.2"
ThisBuild / javaOptions += "-Xmx32G"

lazy val root = crossProject(JVMPlatform).withoutSuffixFor(JVMPlatform)
  .in(file("."))
  .jvmSettings(
    ThisBuild / fork := true
  )
  .settings(
    name := "CZ2-DB",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0-M10" % Test,
    libraryDependencies += "be.adamv" %% "cz2" % "0.2.15",
    libraryDependencies += "com.lihaoyi" %% "cask" % "0.9.5",
//    nativeConfig ~= {
//      _.withLTO(LTO.full)
//        .withMode(Mode.releaseFull)
//        .withGC(GC.commix)
//    }
)
