/* 
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
