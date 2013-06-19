/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import java.util.List;
import se.sics.caracaldb.Key;
import se.sics.kompics.Response;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class LookupResponse extends Response {

    public final Key key;
    public final long reqId;
    public final List<Address> hosts;

    public LookupResponse(LookupRequest request, Key key, long reqId,
            List<Address> hosts) {
        super(request);
        this.key = key;
        this.reqId = reqId;
        this.hosts = hosts;
    }
}
