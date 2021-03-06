package se.sics.caracaldb.driver

import akka.actor.{Actor, ActorLogging}
import akka.pattern.pipe
import se.sics.kompics._
import se.sics.kompics.ComponentProxy
import se.sics.caracaldb.experiment.dataflow.MessageRegistrator
import com.google.common.util.concurrent._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

case class CreateComponent(setup: ComponentSetup)
case class Created(component: Component)
case class DestroyComponent(component: Component)

class CreateKompicsComponent(val setup: ComponentSetup, val promise: SettableFuture[Component]) extends KompicsEvent
class DestroyKompicsComponent(val component: Component) extends KompicsEvent

class KompicsActor extends Actor with ActorLogging {
    import com.larskroll.common.FutureConversions._
    import context._

    MessageRegistrator.register();
    log.debug("Starting Kompics...");
    Kompics.createAndStart(classOf[ActorComponent], Runtime.getRuntime().availableProcessors());

    def component = {
        val f = KompicsActor.componentF.toPromise;
        Await.result(f, Duration.Inf);
    }

    def receive = {
        case CreateComponent(setup) => {
            val sf = SettableFuture.create[Component];
            component.triggerOnSelf(new CreateKompicsComponent(setup, sf));
            val f: Future[Component] = sf.toPromise;
            f map { Created(_) } pipeTo sender
        }
        case DestroyComponent(c) => {
            component.triggerOnSelf(new DestroyKompicsComponent(c))
        }
    }
    
    override def postStop {
        log.debug("Sopping Kompics...");
        Kompics.shutdown();
    }
}

object KompicsActor {

    val componentF = SettableFuture.create[ActorComponent];
}

class ActorComponent extends ComponentDefinition {
    import ActorComponent._;

    val startHandler = new Handler[Start] {
        def handle(event: Start) {
            KompicsActor.componentF.set(ActorComponent.this);
            LOG.debug("KompicsActor ready.");
        }
    }
    subscribe(startHandler, control);

    val createHandler = new Handler[CreateKompicsComponent] {
        def handle(event: CreateKompicsComponent) {
            val c = event.setup(proxy);
            event.promise.set(c);
        }
    }
    subscribe(createHandler, loopback);
    
    val destroyHandler = new Handler[DestroyKompicsComponent] {
        def handle(event: DestroyKompicsComponent) {
            trigger(Kill.event, event.component.control());
        }
    }
    subscribe(destroyHandler, loopback);

    def triggerOnSelf(event: KompicsEvent) {
        trigger(event, onSelf);
    }
}

object ActorComponent {
    val LOG = LoggerFactory.getLogger(classOf[ActorComponent]);
}
