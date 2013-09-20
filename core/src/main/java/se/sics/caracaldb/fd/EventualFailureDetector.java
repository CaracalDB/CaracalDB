/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.fd;

import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class EventualFailureDetector extends PortType {{
    request(SubscribeNodeStatus.class);
    request(UnsubscribeNodeStatus.class);
    indication(Suspect.class);
    indication(Restore.class);
}}
