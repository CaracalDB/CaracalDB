package se.sics.caracaldb.driver.experiments

import akka.actor.{Status=>_, _}
import akka.pattern.ask
import scala.concurrent._
import scala.concurrent.duration._
import se.sics.caracaldb.driver._
import se.sics.caracaldb.experiment.dataflow._
import se.sics.kompics.network.Transport
import java.io.FileWriter
import java.io.File
import grizzled.math._

class TorrentTransfer extends Experiment[TransferData] {
    
    private var sender: ActorRef = null;
    private var receiver: ActorRef = null;
    private var file: File = null;
    private var rseTarget = 0.01;
    val timeseries = scala.collection.mutable.ArrayBuffer.empty[Long];

    def postRun(system: ActorSystem): Boolean = {
        import Numeric._
        import Math.sqrt
        
        receiver ! Cleanup
        if (timeseries.size <= 10) {
            system.log.info(f"Need at least 10 samples for SSD formula. Got ${timeseries.size}%d.");
            return true
        } 
        val ssd = stats.sampleStandardDeviation(timeseries: _*);
        val mean = stats.mean(timeseries: _*);    
        val sem = ssd/sqrt(timeseries.length); // standard error of the mean
        val rse = sem/mean; // relative standard error
        system.log.info(f"Samples = ${timeseries.length}%d, µ = $mean%f, SSD = $ssd%f, SEM = $sem%f, RSE = $rse%f (target $rseTarget%f)");
        return rse > rseTarget;
    }
    def preRun(system: ActorSystem) {}
    def run(system: ActorSystem): TransferData = {
        implicit val ec: ExecutionContext = system.dispatcher;
        
        val rF = for {
            addr <- (sender ? GetAddress).mapTo[SenderAddress]
            data <- (receiver ? Transfer(addr.addr, null)).mapTo[TransferStats]
        } yield data

        val rt = Await.result(rF, Duration.Inf);
        timeseries += rt.time;
        TransferData(rt.time, rt.bytes, rt.averageThroughput)
    }
    
    def createSR(system: ActorSystem)(implicit ec: ExecutionContext): Future[Tuple2[ActorRef, ActorRef]] = {
        val sender = system.actorOf(Props(classOf[TorrentSenderActor]), "sender");
        val receiver = system.actorOf(Props(classOf[TorrentReceiverActor]), "receiver");
        val senderReady = sender ? Status;
        val receiverReady = receiver ? Status;
        for {
            s <- senderReady
            r <- receiverReady
        } yield (sender, receiver)
    }
    
    def setUp(system: ActorSystem) {
        implicit val ec: ExecutionContext = system.dispatcher;
        
        val f = createSR(system);
        val (s, r) = Await.result(f, Duration.Inf);
        sender = s;
        receiver = r;
        rseTarget = system.settings.config.getDouble("experiment.rseTarget");
    }
    def tearDown(system: ActorSystem) {
        if (sender != null) {
            sender ! PoisonPill
            sender = null
        }
        if (receiver != null) {
            receiver ! PoisonPill
            receiver = null
        }
        if (file != null) {
            file = null;
        }
        timeseries.clear();
    }
    def identifier: String = "TorrentExperiment"
}