package su.test.rep

import java.net.InetSocketAddress

import akka.actor.{ActorRef, ActorSystem}

object Starter extends App {
  if (args.length < 3) {
    println("Random purchase events producer. Usage: <host> <port> <recordsCount>")
    sys.exit(-1)
  }

  val system: ActorSystem = ActorSystem("REP")
  system.log.info("Starting REP with params host: [{}], port: [{}], records count: [{}]", args(0), args(1), args(2))
  val producerActor: ActorRef = system.actorOf(Producer.props(args(2).toInt), "RecordProducer")
  val tcpClient: ActorRef = system.actorOf(Client.props(new InetSocketAddress(args(0), args(1).toInt), producerActor), "TcpClient")

}


