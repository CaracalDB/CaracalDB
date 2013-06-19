/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos;

import com.google.common.collect.ImmutableSet;
import se.sics.caracaldb.View;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class PaxosInit extends Init<Paxos> {
    
    public final View view;
    public final int quorum;
    public final long networkBound;
    public final Address self;
    
    public PaxosInit(View v, int quorum, long networkBound, Address self) {
        this.view = v;
        this.quorum = quorum;
        this.networkBound = networkBound;
        this.self = self;
    }
    
}
