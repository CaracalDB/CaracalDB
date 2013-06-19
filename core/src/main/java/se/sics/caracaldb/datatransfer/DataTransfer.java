/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DataTransfer extends PortType {{
    request(StatusRequest.class);
    indication(StatusResponse.class);
    indication(Completed.class);
}}
