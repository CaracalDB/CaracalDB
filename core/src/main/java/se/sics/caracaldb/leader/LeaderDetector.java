/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.leader;

import se.sics.caracaldb.paxos.Reconfigure;
import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class LeaderDetector extends PortType {
    {
        request(Reconfigure.class);
        indication(Trust.class);
    }
}
