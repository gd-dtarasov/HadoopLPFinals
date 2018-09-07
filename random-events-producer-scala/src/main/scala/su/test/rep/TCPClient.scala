package su.test.rep

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io.Tcp.Event
import akka.io.{IO, Tcp}
import akka.util.ByteString

object Client {
  def props(remote: InetSocketAddress, replies: ActorRef) =
    Props(classOf[Client], remote, replies)
}

case object Ack extends Event

class Client(remote: InetSocketAddress, listener: ActorRef) extends Actor with ActorLogging {

  import Tcp._
  import context.system
  
  
  override def preStart() {
    IO(Tcp) ! Connect(remote)
  }

  def receive = {
    case c@CommandFailed(_: Connect) =>
      log.error("Command failed [{}]", c)
      listener ! CommandFailed
      context stop self

    case Connected(remote, local) =>
      log.info("Connected")
      listener ! Connected
      val connection = sender()
      connection ! Register(self)
      context become {
        case Ack =>
          listener ! Ack
        case data: ByteString =>
          connection ! Write(data, Ack)
        case CommandFailed(w: Write) =>
          log.error("Write failed")
          listener ! "write failed"
        case Received(data) =>
          log.info("Received data [{}]", data.decodeString(StandardCharsets.UTF_8))
          listener ! data
        case "close" =>
          log.info("Closing connection")
          connection ! ConfirmedClose
        case _: ConnectionClosed =>
          log.info("Connection closed")
          listener ! "connection closed"
          context stop self
      }

    case _ => log.error("Something weird happened")
  }
}