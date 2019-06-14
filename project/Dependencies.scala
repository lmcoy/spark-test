import sbt._

object Dependencies {

  object Version {
    lazy val scalaTest = "3.0.5"
    lazy val scalactic = "3.0.5"
    lazy val typesafeConfig = "1.3.4"
    lazy val spark = "2.4.3"
    lazy val sparkTesting = s"${spark}_0.12.0"
  }

  lazy val scalaTest = "org.scalatest" %% "scalatest" % Version.scalaTest

  lazy val scalactic =  "org.scalactic" %% "scalactic" % Version.scalactic
  lazy val typesafeConfig = "com.typesafe" % "config" % Version.typesafeConfig
  
  val spark = "org.apache.spark" %% "spark-sql" % Version.spark

  val sparkTesting = "com.holdenkarau" %% "spark-testing-base" % Version.sparkTesting

  lazy val dependencies = Seq(scalaTest % Test, scalactic, typesafeConfig, spark, sparkTesting % Test)
  
  lazy val dependenciesTesting = Seq(scalaTest, scalactic, typesafeConfig, spark, sparkTesting)
}
