package misc.akka_test

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.FlatSpec

class AkkaRemoteTest extends FlatSpec with LazyLogging {

  "akka" should "remote ping/pong" in {
    import AkkaRemoteTest._
    val system = ActorSystem("remote")
    val remoteActor = system.actorOf(Props[RemoteActor], name = "RemoteActor")
    remoteActor ! "The RemoteActor is alive"

    val localActor = system.actorOf(Props[LocalActor], name = "LocalActor") // the local actor
    localActor ! "START"

    while (true) {
      Thread.sleep(1000)
    }
  }

}

object AkkaRemoteTest {

  class RemoteActor extends Actor {
    def receive = {
      case msg: String =>
        println(s"RemoteActor received message '$msg'")
        sender ! "Hello from the RemoteActor"
    }
  }

  class LocalActor extends Actor {

    val remote = context.actorSelection("akka.tcp://remote@127.0.0.1:2552/user/RemoteActor")
    var counter = 0

    def receive = {
      case "START" =>
        remote ! "Hello from the LocalActor"
      case msg: String =>
        println(s"LocalActor received message: '$msg'")
        if (counter < 5) {
          sender ! "Hello back to you"
          counter += 1
        }
    }
  }

}