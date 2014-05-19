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
package se.sics.datamodel.operations;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.kompics.Event;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public abstract class DMParallelOp extends DMOperation implements DMOperationsManager {
    protected final DMOperationsManager operationsManager;

    protected Map<UUID, DMOperation> pendingOps; //<opId, op>
    protected Map<UUID, UUID> pendingReqs; //<reqId, opId>

    DMParallelOp(UUID id, DMOperationsManager operationsManager) {
        super(id);
        this.operationsManager = operationsManager;
        this.pendingOps = new HashMap<UUID, DMOperation>();
        this.pendingReqs = new HashMap<UUID, UUID>();
    }

    //*****DMOperation*****
    @Override
    public final void handleMessage(CaracalResponse resp) {
        if(done) {
            LOG.debug("Operation {} - finished and will not process further messages", toString());
            operationsManager.droppedMessage(resp);
            return;
        }
        if (pendingReqs == null || pendingOps == null) {
            LOG.warn("Operation {} logic error", toString());
            operationsManager.droppedMessage(resp);
            return;
        }
        UUID opId = pendingReqs.get(resp.id);
        if (opId == null) {
            operationsManager.droppedMessage(resp);
            return;
        }
        DMOperation op = pendingOps.get(opId);
        if (op == null) {
            LOG.warn("Operation {} cleanup error", toString());
            return;
        }
        op.handleMessage(resp);
    }

    //*****DMOperationsMaster****
    @Override
    public void send(UUID opId, UUID reqId, CaracalOp req) {
        pendingReqs.put(reqId, opId);
        operationsManager.send(id, reqId, req);
    }

    @Override
    public void droppedMessage(CaracalOp resp) {
        operationsManager.droppedMessage(resp);
    }
    
    //***** *****
    protected final void cleanChildOp(UUID opId) {
        pendingOps.remove(opId);
        Iterator<Map.Entry<UUID, UUID>> it = pendingReqs.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<UUID, UUID> e = it.next();
            if(e.getValue().equals(opId)) {
                it.remove();
            }
        }
    }
    
    protected final void fullCleanup() {
        pendingOps = null;
        pendingReqs = null;
    }
    
    protected final void finish(DMOperation.Result result) {
        LOG.debug("Operation {} - finished {}", new Object[]{toString(), result.responseCode});
        done = true;
        operationsManager.childFinished(id, result);
    }
}