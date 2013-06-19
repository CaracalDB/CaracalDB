/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import se.sics.caracaldb.persistence.Database;
import se.sics.kompics.Init;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class PersistentStoreInit extends Init<PersistentStore> {
    public final Database db;
    
    public PersistentStoreInit(Database db) {
        this.db = db;
    }
}
