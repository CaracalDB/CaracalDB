/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.kompics.Request;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class LookupRequest extends Request {

    public final byte[] key;
    public final long reqId;

    public LookupRequest(byte[] key, long reqId) {
        this.key = key;
        this.reqId = reqId;
    }
}
