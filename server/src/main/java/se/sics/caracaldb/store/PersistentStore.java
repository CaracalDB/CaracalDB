/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
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
            try {
                Response resp = event.execute(db);
                if (resp != null) {
                    trigger(resp, store);
                }
            } catch (Throwable ex) {
                LOG.error("Exception during process", ex);
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