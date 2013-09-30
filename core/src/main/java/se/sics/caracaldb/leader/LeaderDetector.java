/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.leader;

import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class LeaderDetector extends PortType {
    {
        request(ReconfigureGroup.class);
        indication(Trust.class);
    }
}
