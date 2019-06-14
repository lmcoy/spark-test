lazy val org = "com.example"

ThisBuild / scalaVersion := "2.12.8"

ThisBuild / organization := org
ThisBuild / organizationName := "example"
scapegoatVersion in ThisBuild := "1.3.8"

val gitCommitString = SettingKey[String]("gitCommit")

gitCommitString := git.gitHeadCommit.value.getOrElse("Not Set")




lazy val root = (project in file("test"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "myproject",
    libraryDependencies ++= Dependencies.dependencies,
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, gitCommitString),
    buildInfoPackage := org,
    scalacOptions ++= Seq(
      "-encoding",
      "utf8",
      "-Xfatal-warnings",
      "-deprecation",
      "-unchecked",
      "-language:implicitConversions",
      "-language:higherKinds"
    )
  ).dependsOn(sparktest % Test)

lazy val sparktest: Project = (project in file("spark-test"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "spark-test",
    libraryDependencies ++= Dependencies.dependenciesTesting,
    scalacOptions ++= Seq(
      "-encoding",
      "utf8",
      "-Xfatal-warnings",
      "-deprecation",
      "-unchecked",
      "-language:implicitConversions",
      "-language:higherKinds"
    )
  )
