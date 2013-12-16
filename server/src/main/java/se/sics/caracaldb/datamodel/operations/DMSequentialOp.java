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
package se.sics.caracaldb.datamodel.operations;

import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.kompics.Event;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public abstract class DMSequentialOp extends DMOperation implements DMOperationsMaster {

    protected final DMOperationsMaster operationsMaster;

    protected DMOperation pendingOp;

    protected DMSequentialOp(long id, DMOperationsMaster operationsMaster) {
        super(id);
        this.operationsMaster = operationsMaster;
    }

    //*****OMOperation*****
    @Override
    public void handleMessage(CaracalResponse resp) {
        if (pendingOp != null) {
            pendingOp.handleMessage(resp);
        } else {
            operationsMaster.droppedMessage(resp);
        }
    }

    //*****OMOperator*****
    @Override
    public void send(long opId, long reqId, Event req) {
        operationsMaster.send(id, reqId, req);
    }

    @Override
    public void droppedMessage(Event message) {
        operationsMaster.droppedMessage(message);
    }
}