/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

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
