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
import scala.language.postfixOps
import se.sics.caracaldb.experiment.torrent.Receiver
import se.sics.caracaldb.experiment.dataflow.TransferStats

class TorrentReceiverActor extends Actor with ActorLogging {
    import context._
    import com.larskroll.common.FutureConversions._

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
                null, // sender address to be passed later
                outputDir,
                resultQ);
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
        case Transfer(sendr, file) => {
            val c = component;
            val recvComponent = c.getComponent().asInstanceOf[Receiver];
            val f = recvComponent.startTransfer(sendr).toPromise;
            f pipeTo sender
        }
        case Cleanup    => FileUtils.cleanDirectory(outputDir);
        case x          => println(x)
    }
}