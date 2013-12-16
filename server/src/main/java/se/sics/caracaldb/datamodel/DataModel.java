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
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.datamodel.msg.GetAllTypes;
import se.sics.caracaldb.datamodel.msg.GetGsonObj;
import se.sics.caracaldb.datamodel.msg.GetType;
import se.sics.caracaldb.datamodel.msg.PutGsonObj;
import se.sics.caracaldb.datamodel.msg.PutType;
import se.sics.caracaldb.datamodel.operations.DMOperationsMaster;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Event;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class DataModel extends ComponentDefinition implements DMOperationsMaster {
    private static final Logger LOG = LoggerFactory.getLogger(DataModel.class);
    
    Negative<DataModelPort> dataModel = provides(DataModelPort.class);
    
    public DataModel(DataModelInit init) {
//        subscribe(requestHandler, dataModel);
        subscribe(getAllTypesHandler, dataModel);
        subscribe(getTypeHandler, dataModel);
        subscribe(putTypeHandler, dataModel);
        subscribe(getGsonObjHandler, dataModel);
        subscribe(putGsonObjHandler, dataModel);
    }
    
    Handler<GetAllTypes.Req> getAllTypesHandler = new Handler<GetAllTypes.Req>() {

        @Override
        public void handle(GetAllTypes.Req event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    };
    
    Handler<GetType.Req> getTypeHandler = new Handler<GetType.Req>() {

        @Override
        public void handle(GetType.Req event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    };
    
    Handler<PutType.Req> putTypeHandler = new Handler<PutType.Req>() {

        @Override
        public void handle(PutType.Req event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    };
    
    Handler<GetGsonObj.Req> getGsonObjHandler = new Handler<GetGsonObj.Req>() {

        @Override
        public void handle(GetGsonObj.Req event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    };
    
    Handler<PutGsonObj.Req> putGsonObjHandler = new Handler<PutGsonObj.Req>() {

        @Override
        public void handle(PutGsonObj.Req event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    };

    //*****DMOperationsMaster*****
    @Override
    public void send(long opId, long reqId, Event req) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void childFinished(long opId, DMMessage.ResponseCode opResult) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void droppedMessage(Event msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}