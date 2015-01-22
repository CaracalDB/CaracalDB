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
package se.sics.caracaldb.system;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.global.LookupService;
import se.sics.caracaldb.global.MaintenanceService;
import se.sics.caracaldb.global.SchemaData.SingleSchema;
import se.sics.caracaldb.persistence.Database;
import se.sics.caracaldb.store.Store;
import se.sics.kompics.Component;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.VirtualNetworkChannel;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class VirtualSharedComponents extends ServiceRegistry {

    public VirtualSharedComponents(byte[] id, SingleSchema schema) {
        this.id = id;
        this.schema = schema;
    }
    /*
     * Address
     */
    private byte[] id;
    private Address self;
    private SingleSchema schema;
    private Class<? extends Database> dbType = null;

    void setSelf(Address self) {
        this.self = self;
    }

    public Address getSelf() {
        return self;
    }

    public byte[] getId() {
        return id;
    }
    
    public SingleSchema getSchema() {
        return this.schema;
    }
    
    public Class<? extends Database> getDbType(Configuration config) throws ClassNotFoundException {
        if (dbType != null) {
            return dbType;
        }
        String dbName = schema.meta.get("db");
        if (dbName == null) {
            dbName = "leveldb"; // default value
        }
        dbType = config.getDBMan().getType(dbName);
        return dbType;
    }
    
    public Database.Level getDbLevel(Configuration config) throws ClassNotFoundException {
        Class<? extends Database> dbtype = getDbType(config);
        try {
            Method m = dbtype.getMethod("level");
            return (Database.Level) m.invoke(null);
        } catch (Exception ex) {
            HostManager.LOG.error("Could not find level information for database {}. Error was: \n{}", dbtype, ex);
            throw new ClassNotFoundException(ex.getMessage());
        }
    }
    
    /*
     * Core Services
     */
    private VirtualNetworkChannel net;
    private Positive<LookupService> lookup;
    private Positive<Store> store;
    private Positive<MaintenanceService> maintenance;
    private Positive<Timer> timer;
    private Positive<EventualFailureDetector> fd;

    void setNetwork(VirtualNetworkChannel vnc) {
        this.net = vnc;
    }
    
    void setFailureDetector(Positive<EventualFailureDetector> fd) {
        this.fd = fd;
    }

    public void connectNetwork(Component c) {
        net.addConnection(id, c.getNegative(Network.class));
    }

    public void disconnectNetwork(Component c) {
        net.removeConnection(id, c.getNegative(Network.class));
    }

    public void setLookup(Positive<LookupService> lookup) {
        this.lookup = lookup;
    }

    public Positive<LookupService> getLookup() {
        return lookup;
    }

    public void setStore(Positive<Store> store) {
        this.store = store;
    }

    public Positive<Store> getStore() {
        return store;
    }

    /**
     * @return the maintenance
     */
    public Positive<MaintenanceService> getMaintenance() {
        return maintenance;
    }

    /**
     * @param maintenance the maintenance to set
     */
    public void setMaintenance(Positive<MaintenanceService> maintenance) {
        this.maintenance = maintenance;
    }

    /**
     * @return the timer
     */
    public Positive<Timer> getTimer() {
        return timer;
    }

    /**
     * @param timer the timer to set
     */
    public void setTimer(Positive<Timer> timer) {
        this.timer = timer;
    }
    
    public Positive<EventualFailureDetector> getFailureDetector() {
        return this.fd;
    }
}
