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
package se.sics.caracaldb.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.persistence.Database;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Response;
import se.sics.kompics.Stop;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class PersistentStore extends ComponentDefinition {
    
    private static final Logger LOG = LoggerFactory.getLogger(PersistentStore.class);
    
    Negative<Store> store = provides(Store.class);
    
    private Database db;
    
    public PersistentStore(PersistentStoreInit init) {
        this.db = init.db;
        
        // subscriptions
        subscribe(requestHandler, store);
        subscribe(stopHandler, control);
        
    }
    
    Handler<StorageRequest> requestHandler = new Handler<StorageRequest>() {
        @Override
        public void handle(StorageRequest event) {
            Response resp = event.execute(db);
            if (resp != null) {
                trigger(resp, store);
            }
        }        
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            db.close();
        }        
    };
    
}
