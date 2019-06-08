import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "calcite-playground",
    libraryDependencies ++= runtimeDeps,
    libraryDependencies += scalaTest % Test
  )
