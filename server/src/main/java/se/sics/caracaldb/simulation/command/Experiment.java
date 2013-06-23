/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.simulation.command;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Experiment extends PortType {

    {
        indication(BootCmd.class);
        indication(TerminateCmd.class);
        indication(ValidateCmd.class);
        indication(OpCmd.class);
        
        positive(TerminateExperiment.class);
        negative(TerminateExperiment.class);
    }
}
