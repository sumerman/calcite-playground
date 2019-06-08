import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val runtimeDeps = Seq(
    "org.apache.calcite" % "calcite-core" % "1.19.0",
    "com.typesafe" % "config" % "1.3.4",
    "org.postgresql" % "postgresql" % "42.2.5"
  )
}
