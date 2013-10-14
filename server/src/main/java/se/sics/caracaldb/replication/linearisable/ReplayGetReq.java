/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.store.GetReq;
import se.sics.caracaldb.store.GetResp;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ReplayGetReq extends GetReq {
    
    private static final Logger LOG = LoggerFactory.getLogger(ReplayGetReq.class);
    
    private final List<CaracalOp> ops;
    
    public ReplayGetReq(Key k, List<CaracalOp> ops) {
        super(k);
        this.ops = ops;
    }
    
    @Override
    public Response execute(Persistence store) {
        GetResp resp = (GetResp) super.execute(store);
        byte[] val = resp.value;
        for (CaracalOp op : ops) {
            if (op instanceof PutRequest) {
                PutRequest pr = (PutRequest) op;
                if (!key.equals(pr.key)) {
                    LOG.warn("Op {} should not have been in list for {}", pr, this);
                    continue;
                }
                val = pr.data;
            } else {
                LOG.warn("Op {} is not a PUT. Forgot to add a new operation?", op, this);
            }
        }
        resp = new GetResp(this, key, val);
        return resp;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(" replay [");
        for (CaracalOp op : ops) {
            sb.append(op.toString());
            sb.append("\n   ");
        }
        sb.append("]\n");
        return sb.toString();
    }
}
