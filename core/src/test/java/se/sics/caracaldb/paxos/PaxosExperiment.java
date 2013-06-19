/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class PaxosExperiment extends PortType {

    {
        indication(Commands.Start.class);
        indication(Commands.Verify.class);
        indication(Commands.Operation.class);
        indication(Commands.Join.class);
        indication(Commands.Fail.class);

        positive(TerminateExperiment.class);
        negative(TerminateExperiment.class);
    }
}
