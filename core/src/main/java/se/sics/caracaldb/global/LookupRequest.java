/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.caracaldb.Key;
import se.sics.kompics.Request;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class LookupRequest extends Request {

    public final Key key;
    public final long reqId;

    public LookupRequest(Key key, long reqId) {
        this.key = key;
        this.reqId = reqId;
    }
}
