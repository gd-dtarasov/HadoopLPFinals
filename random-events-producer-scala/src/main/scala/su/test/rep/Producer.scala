package su.test.rep

import akka.actor.{Actor, Props}
import akka.io.Tcp.{CommandFailed, Connected}
import akka.util.ByteString

object Producer {
  def props(recordCount: Int) = Props(classOf[Producer], recordCount)
}

class Producer(recordCount: Int) extends Actor {
  var sendCount: Int = 0

  override def receive = {
    case CommandFailed => context stop self
    case Connected | Ack =>
      val client = sender()
      val data: ByteString = ByteString.apply(RecordGenerator.generatePurchaseRecord)
      client ! data
      sendCount += 1
      if (sendCount == recordCount) {
        client ! "close"
      }
  }
}
