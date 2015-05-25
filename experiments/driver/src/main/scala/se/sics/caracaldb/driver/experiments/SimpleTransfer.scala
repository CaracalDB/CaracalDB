package se.sics.caracaldb.driver.experiments

import akka.actor._
import akka.pattern.ask
import scala.concurrent._
import scala.concurrent.duration._
import se.sics.caracaldb.driver._
import se.sics.caracaldb.experiment.dataflow._
import se.sics.kompics.network.Transport
import java.io.FileWriter
import java.io.File
import grizzled.math._

case class TransferData(runtime: Long, datavolume: Long, throughput: Double) extends ExperimentData {
    def appendToFile(fw: FileWriter) {
        fw.write(f"$runtime%d, $datavolume%d, $throughput%f\n");
    }
}

class SimpleTransfer(val transport: Transport) extends Experiment[TransferData] {
    
    private var sender: ActorRef = null;
    private var receiver: ActorRef = null;
    private var file: File = null;
    private var ssdTarget = 0.01;
    val timeseries = scala.collection.mutable.ArrayBuffer.empty[Long];

    def postRun(system: ActorSystem): Boolean = {
        import Numeric._
        
        receiver ! Cleanup
        if (timeseries.size <= 10) {
            system.log.info(f"Need at least 10 samples for SSD formula. Got ${timeseries.size}%d.");
            return true
        } 
        val ssd = stats.sampleStandardDeviation(timeseries: _*);
        val mean = stats.mean(timeseries: _*);    
        val relssd = ssd/mean;
        system.log.info(f"Samples = ${timeseries.size}%d, Mean = $mean%f, SSD = $ssd%f, Relative SSD = $relssd%f (target $ssdTarget%f)");
        return relssd > ssdTarget;
    }
    def preRun(system: ActorSystem) {}
    def run(system: ActorSystem): TransferData = {
        implicit val ec: ExecutionContext = system.dispatcher;
        
        val rF = for {
            addr <- (receiver ? GetAddress).mapTo[ReceiverAddress]
            data <- (sender ? Transfer(addr.addr, file)).mapTo[StatsWithRTTs]
        } yield data

        val rt = Await.result(rF, Duration.Inf);
        val r = rt.stats;
        timeseries += r.time;
        TransferData(r.time, r.bytes, r.averageThroughput)
    }
    def setUp(system: ActorSystem) {
        implicit val ec: ExecutionContext = system.dispatcher;

        file = new File(system.settings.config.getString("experiment.file"));
        
        val f = createSR(system, transport, null);
        val (s, r) = Await.result(f, Duration.Inf);
        sender = s;
        receiver = r;
        ssdTarget = system.settings.config.getDouble("experiment.ssdTarget");
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
    def identifier: String = "SimpleTransferExperiment"+transport
}