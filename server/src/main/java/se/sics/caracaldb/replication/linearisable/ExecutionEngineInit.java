/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ExecutionEngineInit extends Init<ExecutionEngine> {
    public final View view;
    public final Address self;
    public final KeyRange range;
    public final long keepAlivePeriod;
    public final int dataMessageSize;
    public ExecutionEngineInit(View v, Address self, KeyRange range, long keepAlivePeriod, int dataMessageSize) {
        this.view = v;
        this.self = self;
        this.range = range;
        this.keepAlivePeriod = keepAlivePeriod;
        this.dataMessageSize = dataMessageSize;
    }
}
