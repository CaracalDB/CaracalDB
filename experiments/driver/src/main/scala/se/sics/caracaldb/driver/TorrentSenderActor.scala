package se.sics.caracaldb.driver

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import se.sics.caracaldb.Address
import se.sics.kompics.Start
import se.sics.kompics.network.Transport
import java.net.InetAddress
import java.io.File
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import se.sics.caracaldb.experiment.torrent.Sender;
import se.sics.caracaldb.experiment.dataflow.TransferStats

case class SenderAddress(addr: Address)

class TorrentSenderActor extends Actor with ActorLogging {
    import context._
    import com.larskroll.common.FutureConversions._

    val conf = system.settings.config
    
    val addr = new Address(Main.pubip, Main.generatePort, null);
    
    val kompics = actorSelection("/user/kompics");
    
    implicit val timeout = Timeout(1 hour);
    
    val componentF = (kompics ? CreateComponent { proxy =>
        {
            import proxy._;
            val senderInit = new Sender.Init(addr, new File("."));
            val c = create(classOf[Sender], senderInit);
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
        case GetAddress => sender ! SenderAddress(addr);
        case x => println(x)
    }
    
}