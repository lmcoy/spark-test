// generate scala source from build definitions
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
// linting with scala style
addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "1.0.0")
// scalafmt
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.0")
// git
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
// build fat jars
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
// test coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.0")
// linting with scapegoat
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.0.9")
// scan for dependencies with known vulnerabilities
addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "1.0.0")
