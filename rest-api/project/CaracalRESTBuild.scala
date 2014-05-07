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
      version := "0.2-SNAPSHOT",
      scalaVersion := "2.10.4",
      //scalacOptions += "-Ydependent-method-types",
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-language:postfixOps", "-language:implicitConversions"),
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
      resolvers += "spray repo" at "http://repo.spray.io",
      resolvers += "spray nightly repo" at "http://nightlies.spray.io",
      resolvers += "sonatype releases"  at "https://oss.sonatype.org/content/repositories/releases/",
      resolvers += "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
      //resolvers += "eveapi" at "https://eveapi.googlecode.com/svn/m2/releases",
      libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.0",
      libraryDependencies += "com.typesafe.akka" %%   "akka-slf4j" % "2.2.0",
      libraryDependencies += "com.typesafe.akka" %%   "akka-testkit" % "2.2.0",
      libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.2" % "test",
      //libraryDependencies += "play" %% "anorm" % "2.1.5",
      //libraryDependencies += "com.github.seratch" %% "scalikejdbc"               % "[1.6,)",
      //libraryDependencies += "com.github.seratch" %% "scalikejdbc-interpolation" % "[1.6,)",
      //libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.21",
      libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.5",
      //libraryDependencies += "org.scalaz" %% "scalaz-core" % "6.0.4",
      libraryDependencies += "io.spray" % "spray-can" % "1.2-20131011", // <-- this is supposed to be a SNAPSHOT, but for some reason SBT doesn't resolve it properly -.-
      libraryDependencies += "io.spray" % "spray-caching" % "1.2-20131011",
      libraryDependencies += "io.spray" % "spray-routing" % "1.2-20131011",
      libraryDependencies += "io.spray" % "spray-testkit" % "1.2-20131011",
      libraryDependencies += "io.spray" % "spray-util" % "1.2-20131011",
      libraryDependencies += "io.spray" %% "spray-json" % "1.2.5",
      libraryDependencies += "se.sics.caracaldb" % "caracaldb-core" % "0.0.4-SNAPSHOT",
      libraryDependencies += "se.sics.caracaldb" % "caracaldb-client" % "0.0.4-SNAPSHOT",
      libraryDependencies += "com.google.code.findbugs" % "jsr305" % "2.0.2",
      libraryDependencies += "com.google.inject" % "guice-parent" % "3.0"
      //libraryDependencies += "com.beimin" % "eveapi" % "5.1.2",
      //libraryDependencies += "joda-time" % "joda-time" % "2.0"
      // libraryDependencies += "org.mindrot" % "jbcrypt" % "0.3m",
      // libraryDependencies += "com.oracle" % "ojdbc6" % "11.2.0.3",
      // libraryDependencies += "org.specs2" % "specs2_2.10.0-RC5" % "1.12.3",
      // libraryDependencies += "com.chuusai" %% "shapeless" % "1.2.3" cross CrossVersion.full
    ) ++ seq(Revolver.settings: _*)
  )
}
