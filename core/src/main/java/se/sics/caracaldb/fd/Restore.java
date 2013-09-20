/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.fd;

import se.sics.kompics.Response;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Restore extends Response {

    public final Address node;

    public Restore(SubscribeNodeStatus req, Address node) {
        super(req);
        this.node = node;
    }
}
