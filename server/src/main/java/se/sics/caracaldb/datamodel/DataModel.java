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
package se.sics.caracaldb.datamodel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */


public class DataModel extends ComponentDefinition {
    private static final Logger LOG = LoggerFactory.getLogger(DataModel.class);
    
    Positive<SIDataModelPort> dataModel = requires(SIDataModelPort.class);
    
    public DataModel(DataModelInit init) {
        subscribe(requestHandler, dataModel);
    }
    
    Handler<DMMessage.Req> requestHandler = new Handler<DMMessage.Req>() {

        @Override
        public void handle(DMMessage.Req req) {
            LOG.debug("processing request {}", new Object[]{req});
            DMMessage.Resp resp = new DMMessage.Resp(req.id, ResponseCode.SUCCESS);
            LOG.debug("finished request {}", new Object[]{resp});
            trigger(resp, dataModel);
        }
    };
}