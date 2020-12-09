import Dependencies._

val releaseVersion = "0.1.0-SNAPSHOT"

lazy val sample = (project in file("."))
  .settings(projectSettings)
  .settings(
    name := "spark-sbt-sample",
    moduleName := "spark-sample",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
      "com.github.scopt" %% "scopt" % "3.7.1"
//      "com.gigahex" %% "spark-error" % "0.0.3-SNAPSHOT"
    )
  )

lazy val projectSettings = baseSettings ++ buildSettings ++  Seq(
  organization := "com.gigahex"
)

lazy val buildSettings = Seq(
  version := releaseVersion,
  crossScalaVersions := Seq("2.11.12", "2.12.6"),
  //assemblyOutputPath in assembly := file(s"${baseDirectory.value.getAbsolutePath}/target/${moduleName.value}.jar"),
  assemblyJarName in assembly := s"${moduleName.value}.jar",
  assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf"                            => MergeStrategy.concat
    case "unwanted.txt"                                => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "BUILD" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith ".default" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith "class" => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  scalaModuleInfo := scalaModuleInfo.value.map(_.withOverrideScalaVersion(true)),
  fork in Test := true
)
lazy val baseSettings = Seq(
  libraryDependencies ++= Seq(
    "org.mockito" % "mockito-core" %  versions.mockito % Test,
    "org.scalatest" %% "scalatest" %  versions.scalaTest % Test
  ),
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  ),
  scalaCompilerOptions,
  javacOptions in (Compile, compile) ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked")
)
lazy val scalaCompilerOptions = scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xlint",
  "-Ywarn-unused-import"
)