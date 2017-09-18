package misc

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

class SparkMigrateTest extends FlatSpec with LazyLogging {

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  "spark session" should "simply work" in {

    lazy val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkMigrateTest")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.createDataset(
      Seq(
        "apache spark",
        "apache hadoop"
      )
    )

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    wordCounts.show()

    //only used for streaming input
    /*val query = wordCounts.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()*/
  }

  it should "recreate session" in {
    lazy val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkMigrateTest")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("INFO")

    def runSomeTask(spark: SparkSession) = {
      //run some task
      val lines = spark.createDataset(
        Seq(
          "apache spark",
          "apache hadoop"
        )
      )
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.groupBy("value").count()
      wordCounts.show()
    }

    runSomeTask(spark)

    //spark.close() == spark.stop()
    logger.warn("spark.stop()")
    /**
      * if not:
      * - next session will reuse this session
      *   - WARN SparkSession$Builder: Using an existing SparkSession; some configuration may not take effect.
      */
    spark.stop()

    lazy val spark2 = SparkSession
      .builder
      .master("local")
      .appName("SparkMigrateTest")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()
    import spark2.implicits._
    spark2.sparkContext.setLogLevel("INFO")

    runSomeTask(spark2)
    spark2.stop()
  }

  it should "handle cmd when get message" in {
    lazy val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkMigrateTest")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val input = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    /*val phase2 = input.as[String].flatMap(_.split(" "))
      .groupBy("value").count()*/

    val phase2 = input.as[String].map { x =>
      if (x.startsWith(">")) {
        val cmd = x.substring(1)
        cmd match {
          case "stop" =>
            logger.debug("received stop cmd")
            spark.stop()
            "calling spark.stop()"
          case _ => "not known cmd"
        }
      } else x
    }.groupBy("value").count()

    val query = phase2.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

  it should "create many sessions" in {
    lazy val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkMigrateTest")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()

    import spark.implicits._
    //spark.sparkContext.setLogLevel("WARN")

    val sessionMap = Range(1, 100).map(index => (index, spark.newSession())).toMap

    sessionMap.foreach(println)

    spark.stop()
  }


}
