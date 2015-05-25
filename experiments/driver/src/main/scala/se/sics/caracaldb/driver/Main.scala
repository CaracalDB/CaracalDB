package se.sics.caracaldb.driver

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import se.sics.caracaldb.experiment.dataflow.Receiver
import se.sics.caracaldb.experiment.dataflow.TransferStats
import se.sics.caracaldb.Address
import se.sics.kompics._
import java.net.InetAddress
import java.net.ServerSocket
import java.io.IOException
import java.io.FileWriter
import java.util.concurrent.LinkedBlockingQueue
import se.sics.kompics.network.Transport

object Main extends App {
    import Role._

    //implicit val timeout = Timeout(1 minute);

    val system = ActorSystem("experiment-driver");
    val kompics = system.actorOf(Props(new KompicsActor), "kompics");

    val conf = system.settings.config;

    val role = Role.withName(conf.getString("experiment.role"));

    //val remoteKompics = system.actorSelection(conf.getString("remoteKompics"));

    val pubip = InetAddress.getByName(conf.getString("experiment.address"));
    System.setProperty("altBindIf", conf.getString("experiment.bind-address"));
    if (conf.getBoolean("experiment.udtMon")) {
        System.setProperty("udtMon", "true");
    }
    System.setProperty("udtBuffer", conf.getBytes("experiment.udtBuffer").toString());
    val runBound = conf.getInt("experiment.runBound");

    role match {
        case Experimentor => {

            

            val participant = system.actorOf(Props[ParticipantManager], "participant");

            implicit val ec = system.dispatcher
            system.log.info("Started as experimentor");
            if (args.size > 0) {
                val experiment = args(0).toInt;
                val exp = Experiments.registered(experiment);
                runExperiment(exp);
            } else {
                Experiments.registered.values foreach { exp =>
                    runExperiment(exp);
                }
            }

            system.log.info("All is done. Shutting down.");
            participant ! Shutdown
            system.shutdown();
        }
        case Participant => {
            system.log.info("Started as participant");
        }
    }

    private def runExperiment[T <: ExperimentData](exp: Experiment[T]) {
        exp.setUp(system);
        val id = exp.identifier;
        val dataFile = new FileWriter(id + ".dat");
        var i = 0;
        try {
            var runAgain = true;
            while (runAgain) {
                system.log.info(s"Preparing $id run $i");
                exp.preRun(system);
                system.log.info(s"Running $id run $i");
                val r = exp.run(system);
                r.appendToFile(dataFile);
                dataFile.flush();
                system.log.info(s"Completed $id run $i");
                runAgain = exp.postRun(system);
                i += 1
                if (i > runBound) {
                    runAgain = false; // avoid endless runs for system with big variance
                }
            }

        } finally {
            dataFile.close();
        }
        exp.tearDown(system)
        Thread sleep 5000 // wait a bit
        system.log.info(s"Finished $id in $i runs");
    }

    def generatePort: Int = {
        this.synchronized {
            try {
                val socket = new ServerSocket(0)
                val port = socket.getLocalPort
                socket.close
                port
            } catch {
                case ioe: IOException =>
                    // this happens when trying to open a socket twice
                    // at the same port
                    // try again
                    generatePort
                case se: SecurityException =>
                    // do nothing
                    generatePort
            }
        }
    }
}