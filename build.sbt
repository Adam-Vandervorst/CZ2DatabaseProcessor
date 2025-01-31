import scala.scalanative.build.*

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.2"
ThisBuild / javaOptions += "-Xmx32G"

lazy val root = crossProject(JVMPlatform, NativePlatform).withoutSuffixFor(JVMPlatform)
  .in(file("."))
  .jvmSettings(
    ThisBuild / fork := false
  )
  .settings(
    name := "CZ2",
    libraryDependencies += "org.scalameta" %%% "munit" % "1.0.0-M10" % Test,
    libraryDependencies += "be.adamv" %%% "cz2" % "0.2.12",
    nativeConfig ~= {
      _.withLTO(LTO.full)
        .withMode(Mode.releaseFull)
        .withGC(GC.commix)
    },
  )
