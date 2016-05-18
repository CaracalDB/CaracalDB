package se.sics.caracaldb.driver

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import se.sics.kompics.network.Transport
import java.io.FileWriter

trait ExperimentData {
    def appendToFile(fw: FileWriter);
}

trait Experiment[Data <: ExperimentData] {

    implicit val timeout = Timeout(1 hour);

    def createSR(system: ActorSystem, transport: Transport, pingTransport: Transport)(implicit ec: ExecutionContext): Future[Tuple2[ActorRef, ActorRef]] = {
        val sender = system.actorOf(Props(classOf[SenderActor], transport, pingTransport), "sender");
        val receiver = system.actorOf(Props(classOf[ReceiverActor], transport), "receiver");
        val senderReady = sender ? Status;
        val receiverReady = receiver ? Status;
        for {
            s <- senderReady
            r <- receiverReady
        } yield (sender, receiver)
    }
    
    def setUp(system: ActorSystem);
    def tearDown(system: ActorSystem);
    def preRun(system: ActorSystem);
    def postRun(system: ActorSystem): Boolean; // return true if wanna run again
    def run(system: ActorSystem): Data;
    def identifier: String;
}