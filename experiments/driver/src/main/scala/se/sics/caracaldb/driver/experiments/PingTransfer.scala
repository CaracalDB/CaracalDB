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

case class PingTransferData(runtime: Long, datavolume: Long, throughput: Double, mRTT: Double, vRTT: Double) extends ExperimentData {
    def appendToFile(fw: FileWriter) {
        fw.write(f"$runtime%d, $datavolume%d, $throughput%f, $mRTT%f, $vRTT%f\n");
    }
}

class PingTransfer(val transport: Transport, val pingTransport: Transport) extends Experiment[PingTransferData] {
    
    import scala.collection.JavaConverters._
    import Numeric._
    import Math.sqrt
    
    private var sender: ActorRef = null;
    private var receiver: ActorRef = null;
    private var file: File = null;
    private var rseTarget = 0.01;
    val timeseries = scala.collection.mutable.ArrayBuffer.empty[Long];

    def postRun(system: ActorSystem): Boolean = {
        
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
    def run(system: ActorSystem): PingTransferData = {
        implicit val ec: ExecutionContext = system.dispatcher;
        
        val rF = for {
            addr <- (receiver ? GetAddress).mapTo[ReceiverAddress]
            data <- (sender ? Transfer(addr.addr, file)).mapTo[StatsWithRTTs]
        } yield data

        val rt = Await.result(rF, Duration.Inf);
        val r = rt.stats;
        val rtts = rt.rtts.asScala.map { x => 
            val l: Long = x; // implicit conversion between object and primitive
            l
        };
        val mRTT = stats.mean(rtts: _*);
        val vRTT = stats.sampleStandardDeviation(rtts: _*);
        timeseries += r.time;
        PingTransferData(r.time, r.bytes, r.averageThroughput, mRTT, vRTT)
    }
    def setUp(system: ActorSystem) {
        implicit val ec: ExecutionContext = system.dispatcher;

        file = new File(system.settings.config.getString("experiment.file"));
        
        val f = createSR(system, transport, pingTransport);
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
    def identifier: String = s"PingTransferExperiment$transport-$pingTransport"
}