package se.sics.caracaldb.driver

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import se.sics.caracaldb.Address
import se.sics.kompics.Start
import se.sics.kompics.network.Transport
import java.net.InetAddress
import java.util.concurrent.LinkedBlockingQueue
import java.io.File
import org.apache.commons.io.FileUtils
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import se.sics.caracaldb.experiment.dataflow.Receiver
import se.sics.caracaldb.experiment.dataflow.TransferStats

case object GetAddress
case class ReceiverAddress(addr: Address)
case object Cleanup

class ReceiverActor(val transport: Transport) extends Actor with ActorLogging {
    import context._

    val conf = system.settings.config

    val kompics = actorSelection("/user/kompics");

    val outputDir = {
        val f = new File(conf.getString("experiment.output"));
        if (!f.exists()) {
            f.mkdirs()
        } else {
            FileUtils.cleanDirectory(f);
        }
        f
    }

    implicit val timeout = Timeout(1 minute);

    val addr = new Address(Main.pubip, Main.generatePort, null);
    val resultQ = new LinkedBlockingQueue[TransferStats];

    val componentF = (kompics ? CreateComponent { proxy =>
        {
            import proxy._
            val recvInit = new Receiver.Init(addr,
                conf.getBytes("experiment.bufferSize"),
                conf.getBytes("experiment.minAlloc"),
                conf.getBytes("experiment.maxAlloc"),
                transport,
                resultQ,
                outputDir);
            val c = create(classOf[Receiver], recvInit);
            trigger(Start.event, c.control());
            c
        }
    }).mapTo[Created].map { x => x.component }

    def component = Await.result(componentF, Duration.Inf);

    override def postStop {
        kompics ! DestroyComponent(component)
    }

    def receive = {
        case Status => {
            val c = component; // this will block until the component is actually available
            sender ! Ready
        }
        case GetAddress => sender ! ReceiverAddress(addr);
        case Cleanup    => FileUtils.cleanDirectory(outputDir);
        case x          => println(x)
    }
}