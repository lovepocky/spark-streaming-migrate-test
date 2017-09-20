name := "spark_migrate_change"

//version := "0.1"

scalaVersion := "2.11.11"

/**
  * common settings
  */
lazy val commonTestSettings: Seq[Def.Setting[_]] = Seq(
  javaOptions in Test ++= Seq(
    "-Dlog4j.debug=true"
    //, "-Dspark.master=local"
  )
)

lazy val root = (project in file("."))
  .settings(commonTestSettings)
  .settings(
    libraryDependencies ++= Seq(
      //core
      "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided,test"
      , "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided,test"
      //, "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0" exclude("org.scalatest", "scalatest_2.11")
      , "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0"

      //test
      , "org.scalactic" %% "scalactic" % "3.0.1" % "provided"
      , "org.scalatest" %% "scalatest" % "3.0.1" % "provided,test"
      , "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.2" % "test"

      //file
      , "com.github.pathikrit" % "better-files_2.11" % "2.17.1"

      //log tool
      , "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"

      //misc tools
      , "com.lihaoyi" %% "ammonite-ops" % "1.0.1"

      //json
      , "org.json4s" %% "json4s-jackson" % "3.2.11"
      , "org.json4s" %% "json4s-ext" % "3.2.11"

      //akka
      , "com.typesafe.akka" %% "akka-actor" % "2.5.4"
      , "com.typesafe.akka" %% "akka-remote" % "2.5.4"
    )
  )