import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val runtimeDeps = Seq(
    "org.postgresql" % "postgresql" % "42.2.5",
    "org.apache.calcite" % "calcite-core" % "1.19.0",
    "com.typesafe" % "config" % "1.3.4",
    // just for Resource
    "org.typelevel" %% "cats-core" % "1.6.0",
    "org.typelevel" %% "cats-effect" % "1.3.0"
  )
}
