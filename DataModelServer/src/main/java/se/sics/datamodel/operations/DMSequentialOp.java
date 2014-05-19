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

import java.util.UUID;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import static se.sics.datamodel.operations.DMOperation.LOG;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public abstract class DMSequentialOp extends DMOperation implements DMOperationsManager {

    protected final DMOperationsManager operationsManager;
    protected DMOperation pendingOp;

    protected DMSequentialOp(UUID id, DMOperationsManager operationsManager) {
        super(id);
        this.operationsManager = operationsManager;
        this.done = false;
    }

    //*****DMOperation*****
    @Override
    public final void handleMessage(CaracalResponse resp) {
        if (done) {
            LOG.debug("Operation {} - finished and will not process further messages", toString());
            operationsManager.droppedMessage(resp);
            return;
        }
        if (pendingOp == null) {
            LOG.warn("Operation {} logic error", toString());
            operationsManager.droppedMessage(resp);
            return;
        }
        pendingOp.handleMessage(resp);
    }

    //*****DMOperationManager*****
    @Override
    public final void send(UUID opId, UUID reqId, CaracalOp req) {
        operationsManager.send(id, reqId, req);
    }

    @Override
    public final void droppedMessage(CaracalOp resp) {
        operationsManager.droppedMessage(resp);
    }

    //***** *****
    protected final void fullCleanup() {
        pendingOp = null;
    }
    
    protected final void finish(DMOperation.Result result) {
        LOG.debug("Operation {} - finished {}", new Object[]{toString(), result.responseCode});
        done = true;
        operationsManager.childFinished(id, result);
    }
}
