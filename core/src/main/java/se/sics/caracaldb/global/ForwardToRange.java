/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ForwardToRange extends Event {

    private final RangeQuery.Request rangeReq;
    public final Address src;
    public final RangeQuery.Type execType;
    public final KeyRange range;

    public ForwardToRange(RangeQuery.Request rangeReq, KeyRange range, Address src) {
        this.rangeReq = rangeReq;
        this.src = src;
        this.execType = rangeReq.execType;
        this.range = range;
    }

    public Message getSubRangeMessage(KeyRange subRange, Address dst) {
        return new CaracalMsg(src, dst, rangeReq.subRange(subRange));
    }
}
