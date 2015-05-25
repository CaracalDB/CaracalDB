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

case class PingData(mRTT: Double, vRTT: Double) extends ExperimentData {
    def appendToFile(fw: FileWriter) {
        fw.write(f"$mRTT%f, $vRTT%f\n");
    }
}

class SimplePings(val transport: Transport, val num: Int) extends Experiment[PingData] {
import scala.collection.JavaConverters._
    import Numeric._
    
    private var sender: ActorRef = null;
    private var receiver: ActorRef = null;

    def postRun(system: ActorSystem): Boolean = {
        receiver ! Cleanup
        false
    }
    def preRun(system: ActorSystem) {}
    def run(system: ActorSystem): PingData = {
        implicit val ec: ExecutionContext = system.dispatcher;
        
        val rF = for {
            addr <- (receiver ? GetAddress).mapTo[ReceiverAddress]
            data <- (sender ? Pings(addr.addr, num)).mapTo[StatsWithRTTs]
        } yield data

        val rt = Await.result(rF, Duration.Inf);
        val rtts = rt.rtts.asScala.map { x => 
            val l: Long = x;
            l
        };
        val mRTT = stats.mean(rtts: _*);
        val vRTT = stats.sampleStandardDeviation(rtts: _*);
        PingData(mRTT, vRTT)
    }
    def setUp(system: ActorSystem) {
        implicit val ec: ExecutionContext = system.dispatcher;
        
        val f = createSR(system, transport, transport);
        val (s, r) = Await.result(f, Duration.Inf);
        sender = s;
        receiver = r;
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
    }
    def identifier: String = s"PingExperiment$transport"
}