import sbt._
import sbt.Keys._
import spray.revolver.RevolverPlugin._
import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys

object CaracalRESTBuild extends Build {

  EclipseKeys.skipParents in ThisBuild := false

  lazy val caracalREST = Project(
    id = "caracal-rest-api",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "CaracalDB REST API",
      organization := "se.sics",
      version := "0.3-SNAPSHOT",
      scalaVersion := "2.11.1",
      //scalacOptions += "-Ydependent-method-types",
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:postfixOps", "-language:implicitConversions"),
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
      resolvers += "spray repo" at "http://repo.spray.io",
      resolvers += "spray nightly repo" at "http://nightlies.spray.io",
      resolvers += "sonatype releases"  at "https://oss.sonatype.org/content/repositories/releases/",
      resolvers += "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
      libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.3",
      libraryDependencies += "com.typesafe.akka" %%   "akka-slf4j" % "2.3.3",
      libraryDependencies += "com.typesafe.akka" %%   "akka-testkit" % "2.3.3",
      libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.7" % "test",
      //libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.5",
      libraryDependencies += "io.spray" %% "spray-can" % "1.3.1-20140423", // <-- this is supposed to be a SNAPSHOT, but for some reason SBT doesn't resolve it properly -.-
      libraryDependencies += "io.spray" %% "spray-caching" % "1.3.1-20140423",
      libraryDependencies += "io.spray" %% "spray-routing" % "1.3.1-20140423",
      libraryDependencies += "io.spray" %% "spray-testkit" % "1.3.1-20140423",
      libraryDependencies += "io.spray" %% "spray-util" % "1.3.1-20140423",
      libraryDependencies += "io.spray" %% "spray-json" % "1.2.6",
      libraryDependencies += "se.sics.caracaldb" % "caracaldb-core" % "0.0.6-SNAPSHOT" excludeAll(ExclusionRule(organization = "org.slf4j")) exclude("log4j", "log4j") exclude("commons-logging", "commons-logging"),
      libraryDependencies += "se.sics.caracaldb" % "caracaldb-client" % "0.0.6-SNAPSHOT" excludeAll(ExclusionRule(organization = "org.slf4j")) exclude("log4j", "log4j") exclude("commons-logging", "commons-logging"),
      libraryDependencies += "se.sics.caracaldb" % "DataModelClient" % "0.0.6-SNAPSHOT" excludeAll(ExclusionRule(organization = "org.slf4j")) exclude("log4j", "log4j") exclude("commons-logging", "commons-logging"),
      libraryDependencies += "com.google.code.findbugs" % "jsr305" % "2.0.2",
      libraryDependencies += "com.google.inject" % "guice-parent" % "3.0",
      libraryDependencies += "com.chuusai" %%  "shapeless" % "1.2.4"
    ) ++ seq(Revolver.settings: _*)
  )
}
