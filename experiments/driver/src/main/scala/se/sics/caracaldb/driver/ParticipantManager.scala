package se.sics.caracaldb.driver

import akka.actor._

case object Shutdown

class ParticipantManager extends Actor with ActorLogging {

    def receive = {
        case Shutdown => {
            log.info("All is done. Shutting down.");
            context.system.shutdown();
        }
    }
}