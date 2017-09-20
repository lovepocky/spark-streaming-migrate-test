package misc

import _root_.akka.actor.Actor
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}

class SparkMigrateTest extends FlatSpec with LazyLogging {

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  /**
    * simple tests
    */
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

  it should "listen many socket" in {
    lazy val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkMigrateTest")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()

    import spark.implicits._

    val sessions = (0 until 6).map { index =>
      spark.newSession()
    }

    val querys = sessions.zipWithIndex.map { case (session, index) =>
      session.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9990 + index)
        .load()
        .as[String].groupBy("value").count()
        .writeStream
        .outputMode("complete")
        .format("console")
        .start()
    }

    querys.head.awaitTermination()

  }


  /**
    * use kafka as source
    */
  it should "work" in {
    lazy val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkMigrateTest")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()

    import spark.implicits._
    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.3.13:9092")
      .option("subscribe", "spark_test_1")

    val df = reader.load()

    df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .groupBy("value").count()
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }

  it should "stop self when received cmd" in {
    import akka.actor.{ActorRef, ActorSystem, Props, Actor, Inbox, Address}
    import scala.concurrent.duration._
    import SparkMigrateTest._

    val driverSystemConfig = ConfigFactory.load().getConfig("driverSystem")
    val system = ActorSystem("driver", driverSystemConfig)
    val sessionActor1 = system.actorOf(Props[SessionActor], "actor1")

    sessionActor1.tell(StartSession(), ActorRef.noSender)

    logger.debug(sessionActor1.path.toStringWithAddress(Address("akka.tcp", "driver")))

    import scala.concurrent.ExecutionContext.Implicits.global

    Await.result(system.whenTerminated, Duration.Inf)
  }

  /**
    * memory as source
    */
  "use memory as source" should "work" in {
    import org.apache.spark.sql.execution.streaming.Source
  }

}

object SparkMigrateTest {

  import akka.actor.{ActorRef, ActorSystem, Props, Actor, Inbox, Address}
  import scala.concurrent.duration._

  case class StopSession()

  case class StartSession()

  class SessionActor extends Actor with LazyLogging {

    import org.apache.spark.sql.streaming.StreamingQueryListener
    import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

    lazy val session: SparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("SparkMigrateTest")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()

    import session.implicits._

    session.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        //println("Query made progress: " + queryProgress.progress)
      }
    })

    session.sparkContext.setLogLevel("WARN")
    val reader = session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.3.13:9092")
      .option("subscribe", "spark_test_1")

    val df = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val process = df
      .map { case (key, value) =>
        if (value.startsWith(">")) {
          value.substring(1) match {
            case "stop" =>
              // not serializable
              // self.tell(StopSession(), self)
              //sender().tell(StopSession(), ActorRef.noSender)
              val system = ActorSystem("executor")
              //system.actorSelection(selfPath).tell(StopSession(), ActorRef.noSender)
              system.actorSelection("akka.tcp://driver@127.0.0.1:2552/user/actor1").tell(StopSession(), ActorRef.noSender)
              "received_stop_cmd"
            case _ => "unknown_cmd"
          }
        } else value
      }
      .groupBy("value").count()
    val query = process.writeStream
      //.option("checkpointLocation", "./temp/checkpoint")
      .outputMode("complete")
      .format("console")

    var queryStart: StreamingQuery = _

    override def receive = {
      case StopSession() =>
        logger.warn("driver received stop message")
        logger.info("terminating spark query")
        queryStart.stop()
        while (queryStart.isActive) {
          logger.info("waiting query to stop")
          Thread.sleep(200)
        }
        //no need to stop session
        //logger.info("terminating spark session")
        //queryStart.sparkSession.stop()
        logger.info("terminating akka system")
        context.stop(self)
        context.system.terminate()

      case StartSession() =>
        logger.info("received start message")
        queryStart = query.start()
        //.awaitTermination()
        logger.info("starting spark query")
      case _ => logger.warn("unknown message")
    }
  }

}