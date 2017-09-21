package misc

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.FlatSpec

import scala.reflect.ClassTag

class MultiplexingTest extends FlatSpec with LazyLogging {

  def splitSample[T: ClassTag](rdd: RDD[T], n: Int, seed: Long = 42): Seq[RDD[T]] = {
    Vector.tabulate(n) { j =>
      rdd.mapPartitions { data =>
        scala.util.Random.setSeed(seed)
        data.filter { unused => scala.util.Random.nextInt(n) == j }
      }
    }
  }

  "spark rdd" should "split" in {
    FileStreamSink

  }

  "spark structured streaming" should "dynamic demultiplexing" in {
    import akka.actor.{ActorRef, ActorSystem, Props, Actor, Inbox, Address}
    import scala.concurrent.duration._
    import com.typesafe.config.ConfigFactory
    import MultiplexingTest._

    val driverSystemConfig = ConfigFactory.load().getConfig("driverSystem")
    val system = ActorSystem("driver", driverSystemConfig)
    val sessionActor = system.actorOf(Props(new SessionActor("192.168.3.13:9092", "spark_test_1")), "sessionActor")

    logger.info("starting...")
    sessionActor.tell(StartCommand(), ActorRef.noSender)

    import scala.concurrent.Await
    Await.result(system.whenTerminated, Duration.Inf)
  }

}

object MultiplexingTest {

  import akka.actor.{ActorRef, ActorSystem, Props, Actor, Inbox, Address}

  case class StartQuery(keyword: String)

  case class StartCommand()

  case class StopQuery(keyword: String)

  case class StopAllQuery()

  case class TerminateSession()

  case class UnknownCommand(cmd: String)

  class SessionActor(kafka_brokers: String, kafka_topic: String) extends Actor with LazyLogging {

    override def receive: Receive = {
      case StartCommand() =>
        queryStartMap += "cmd" -> queryCmd.start()
      case StartQuery(keyword) =>
        queryStartMap += keyword ->
          process.filter(!_.startsWith(">"))
            .filter(_.startsWith(keyword))
            .groupBy("value").count()
            .writeStream
            //.option("checkpointLocation", "./temp/checkpoint")
            .outputMode("complete")
            .format("console")
            .start()
      case StopQuery(keyword) =>
        queryStartMap.get(keyword) match {
          case Some(query) =>
            query.stop()
          case None => logger.error(s"cannot found query: $keyword, in StopQuery(keyword)")
        }
      case StopAllQuery() =>
        queryStartMap.filter(_._1 != "cmd").foreach(_._2.stop())
      case TerminateSession() =>
        queryStartMap.foreach(_._2.stop())
        //session.stop()
        context.stop(self)
        context.system.terminate()

    }

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
      .option("kafka.bootstrap.servers", kafka_brokers)
      .option("subscribe", kafka_topic)

    val df = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val process = df
      .map { case (key, value) => value }

    val queryCmd = process
      .filter(_.startsWith(">"))
      .map { case value =>
        if (value.startsWith(">")) {
          import SessionActor._
          val actorRef = system.actorSelection("akka.tcp://driver@127.0.0.1:2552/user/sessionActor")
          value.substring(1) match {
            case "terminate" =>
              actorRef.tell(TerminateSession(), ActorRef.noSender)
            case "stopAll" =>
              actorRef.tell(StopAllQuery(), ActorRef.noSender)
            case cmd if cmd.startsWith("stop ") =>
              actorRef.tell(StopQuery(cmd.substring(5)), ActorRef.noSender)
            case cmd if cmd.startsWith("start ") =>
              actorRef.tell(StartQuery(cmd.substring(6)), ActorRef.noSender)
            case cmd =>
              actorRef.tell(UnknownCommand(cmd), ActorRef.noSender)
          }
        }
        value
      }
      .groupBy("value").count()
      .writeStream
      //.option("checkpointLocation", "./temp/checkpoint")
      .outputMode("complete")
      .format("console")

    import scala.collection.mutable

    val queryStartMap: mutable.Map[String, StreamingQuery] = mutable.Map(
      //"cmd" -> queryCmd.start()
    )

  }

  object SessionActor {
    lazy val system = ActorSystem("executor")
  }


}
