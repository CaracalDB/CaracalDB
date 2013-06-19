/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import se.sics.caracaldb.View;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class PaxosManagerInit extends Init<PaxosManager> {
    public final Address self;
    public final DecisionStore store;
    public final View view;
    public final long networkBound;
    
    public PaxosManagerInit(View view, long networkBound, Address self, DecisionStore store) {
        this.self = self;
        this.store = store;
        this.view = view;
        this.networkBound = networkBound;
    }
}
