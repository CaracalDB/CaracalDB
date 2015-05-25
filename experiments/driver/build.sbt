import com.typesafe.sbteclipse.plugin.EclipsePlugin._

Revolver.settings

val akkaV = "2.4-SNAPSHOT"

val caracalV = "0.0.7-SNAPSHOT"

lazy val commonSettings = Seq(
  organization := "se.sics",
  version := "0.1.0",
  scalaVersion := "2.11.6"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "caracal-experiment-driver",
    resolvers ++= Seq(
    	Resolver.mavenLocal,
    	"Kompics Releases" at "http://kompics.sics.se/maven/repository/",
		"Kompics Snapshots" at "http://kompics.sics.se/maven/snapshotrepository/",
		"Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
    "Typesafe Snapshots" at "http://repo.akka.io/snapshots/",
		"spray repo" at "http://repo.spray.io",
		"spray nightly repo" at "http://nightlies.spray.io",
		"Sonatype OSS Releases"  at "https://oss.sonatype.org/content/repositories/releases/",
		"Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "JCenter" at "http://jcenter.bintray.com/"
    ),
    libraryDependencies ++= Seq(
		"com.typesafe.akka" %% "akka-actor" % akkaV,
      	"com.typesafe.akka" %% "akka-slf4j" % akkaV,
      	"com.typesafe.akka" %% "akka-testkit" % akkaV,
      	"com.typesafe.akka" %% "akka-remote" % akkaV,
      	"org.slf4j" % "slf4j-log4j12" % "1.7.5",
      	"org.scalatest" %% "scalatest" % "2.1.7" % "test",
      	"se.sics.caracaldb" % "caracaldb-core" % caracalV excludeAll(ExclusionRule(organization = "org.slf4j")) exclude("log4j", "log4j") exclude("commons-logging", "commons-logging") exclude("se.sics.caracaldb", "caracaldb-simulator"),
      	"se.sics.caracaldb" % "caracaldb-dataflow" % caracalV excludeAll(ExclusionRule(organization = "org.slf4j")) exclude("log4j", "log4j") exclude("commons-logging", "commons-logging"),
      	//"se.sics.caracaldb" % "caracaldb-client" % caracalV excludeAll(ExclusionRule(organization = "org.slf4j")) exclude("log4j", "log4j") exclude("commons-logging", "commons-logging"),
      	"com.larskroll" %% "common-utils-scala" % "1.0-SNAPSHOT",
      	"com.google.code.findbugs" % "jsr305" % "2.0.2",
      	"com.google.inject" % "guice-parent" % "3.0",
        "org.clapper" %% "grizzled-scala" % "1.3"
      	//"com.chuusai" %%  "shapeless" % "1.2.4"
	)
  )



