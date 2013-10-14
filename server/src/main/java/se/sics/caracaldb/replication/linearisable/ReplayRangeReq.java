/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeReq;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.store.TransformationFilter;
import se.sics.kompics.Response;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ReplayRangeReq extends RangeReq {

    private static final Logger LOG = LoggerFactory.getLogger(ReplayRangeReq.class);
    private final List<CaracalOp> ops;

    public ReplayRangeReq(KeyRange range, Limit.LimitTracker limit, TransformationFilter transFilter, List<CaracalOp> ops) {
        super(range, limit, transFilter);
        this.ops = ops;
    }

    /*
     * TODO be carefull on buffered deletes, you might return less elements.
     * this definetly needs extra code
     */
    @Override
    public Response execute(Persistence store) throws IOException {
        RangeResp resp = (RangeResp) super.execute(store);
        SortedMap<Key, byte[]> result = resp.result;
        for (CaracalOp op : ops) {
            if (op instanceof PutRequest) {
                PutRequest put = (PutRequest) op;
                result.put(put.key, put.data);
            } else {
                LOG.warn("Op {} is not a PUT. Forgot to add a new operation?", op, this);
            }
        }
        return new RangeResp(this, result, resp.readLimit);
    }
}
