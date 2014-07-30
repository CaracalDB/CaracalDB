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

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.store.ActionFactory;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeAction;
import se.sics.caracaldb.store.RangeReq;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.store.StorageResponse;
import se.sics.caracaldb.store.TransformationFilter;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ReplayRangeReq extends RangeReq {

    private static final Logger LOG = LoggerFactory.getLogger(ReplayRangeReq.class);
    private final List<CaracalOp> ops;

    public ReplayRangeReq(KeyRange range, Limit.LimitTracker limit, TransformationFilter transFilter, RangeAction action, List<CaracalOp> ops) {
        super(range, limit, transFilter, action);
        if (action != ActionFactory.noop()) {
            // FIXME fix this!!!
            LOG.error("FIXME: Replaying RangeReq with Actions can have unintended effects!");
        }
        this.ops = ops;
    }

    /*
     * TODO be carefull on buffered deletes, you might return less elements.
     * this definetly needs extra code
     */
    @Override
    public StorageResponse execute(Persistence store) throws IOException {
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
        // TODO fix this class
        return new RangeResp(this, result, resp.readLimit, null);
    }
}
