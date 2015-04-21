import sbt._
import sbt.Keys._
import spray.revolver.RevolverPlugin._
import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys

object CaracalRESTBuild extends Build {

  EclipseKeys.skipParents in ThisBuild := false

  val sprayV = "1.3.3"
  val akkaV = "2.3.9"
  val caracalV = "0.0.7-SNAPSHOT"

  lazy val caracalREST = Project(
    id = "caracal-rest-api",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "CaracalDB REST API",
      organization := "se.sics",
      version := "0.5-SNAPSHOT",
      scalaVersion := "2.11.6",
      publishMavenStyle := true,
      publishTo := Some(Resolver.sftp("SICS Snapshot Repository", "kompics.i.sics.se", "/home/maven/snapshotrepository")),
      //scalacOptions += "-Ydependent-method-types",
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:postfixOps", "-language:implicitConversions"),
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
      resolvers += "spray repo" at "http://repo.spray.io",
      resolvers += "spray nightly repo" at "http://nightlies.spray.io",
      resolvers += "sonatype releases"  at "https://oss.sonatype.org/content/repositories/releases/",
      resolvers += "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
      libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaV,
      libraryDependencies += "com.typesafe.akka" %%   "akka-slf4j" % akkaV,
      libraryDependencies += "com.typesafe.akka" %%   "akka-testkit" % akkaV,
      libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.7" % "test",
      //libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.5",
      libraryDependencies += "io.spray" %% "spray-can" % sprayV,
      libraryDependencies += "io.spray" %% "spray-caching" % sprayV,
      libraryDependencies += "io.spray" %% "spray-routing" % sprayV,
      libraryDependencies += "io.spray" %% "spray-testkit" % sprayV,
      libraryDependencies += "io.spray" %% "spray-util" % sprayV,
      libraryDependencies += "io.spray" %% "spray-json" % "1.3.1",
      libraryDependencies += "se.sics.caracaldb" % "caracaldb-core" % caracalV excludeAll(ExclusionRule(organization = "org.slf4j")) exclude("log4j", "log4j") exclude("commons-logging", "commons-logging"),
      libraryDependencies += "se.sics.caracaldb" % "caracaldb-client" % caracalV excludeAll(ExclusionRule(organization = "org.slf4j")) exclude("log4j", "log4j") exclude("commons-logging", "commons-logging"),
      libraryDependencies += "com.larskroll" %% "common-utils-scala" % "1.0-SNAPSHOT",
      libraryDependencies += "com.google.code.findbugs" % "jsr305" % "2.0.2",
      libraryDependencies += "com.google.inject" % "guice-parent" % "3.0",
      libraryDependencies += "com.chuusai" %%  "shapeless" % "1.2.4"
    ) ++ seq(Revolver.settings: _*)
  )
}
