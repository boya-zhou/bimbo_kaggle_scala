name := "bimbo_kaggle_scala_sbt"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" %
    "2.1.0" %"provided",
  "org.apache.spark" %% "spark-sql" %
    "2.1.0" %"provided",
  "org.apache.spark" %% "spark-mllib" %
    "2.1.0" %"provided"
)