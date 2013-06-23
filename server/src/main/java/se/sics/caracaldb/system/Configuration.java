/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import se.sics.caracaldb.persistence.Database;
import se.sics.caracaldb.persistence.memory.InMemoryDB;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkr@lars-kroll.com>
 */
public class Configuration implements Cloneable {
    
    private boolean boot = true; // for testing
    
    private InetAddress ip;
    private int port;
    private Set<ComponentHook> hostHooks = new HashSet<ComponentHook>();
    private Set<VirtualComponentHook> virtualHooks = new HashSet<VirtualComponentHook>();
    private Address bootstrapServer;
    private int bootThreshold = 3;
    private int bootVNodes = 1;
    private long keepAlivePeriod = 1000;
    private int messageBufferSizeMax = 16 * 1024;
    private int messageBufferSize = messageBufferSizeMax/8;
    private int dataMessageSize = messageBufferSize;
    // Store
    private Database db;
    
    {
        // Use in memory as default
        db = new InMemoryDB();
    }
    
    public void setIp(InetAddress ip) {
        this.ip = ip;
    }
    
    public InetAddress getIp() {
        return ip;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public int getPort() {
        return port;
    }
    
    public void addHostHook(ComponentHook hook) {
        hostHooks.add(hook);
    }
    
    public void addVirtualHook(VirtualComponentHook hook) {
        virtualHooks.add(hook);
    }
    
    public Iterable<ComponentHook> getComponentHooks() {
        return hostHooks;
    }
    
    public Iterable<VirtualComponentHook> getVirtualHooks() {
        return virtualHooks;
    }
    
    public void setBootThreshold(int n) {
        this.bootThreshold = n;
    }
    
    public int getBootThreshold() {
        return this.bootThreshold;
    }
    
    public void setKeepAlivePeriod(long n) {
        this.keepAlivePeriod = n;
    }
    
    public long getKeepAlivePeriod() {
        return this.keepAlivePeriod;
    }
    
    public void setBootstrapServer(Address addr) {
        this.bootstrapServer = addr;
    }
    
    public Address getBootstrapServer() {
        return this.bootstrapServer;
    }
    
    public boolean isBoot() {
        return boot;
    }
    
    public boolean toggleBoot() {
        if (boot) {
            boot = false;
        } else {
            boot = true;
        }
        
        return boot;
    }
    
    public void setMessageBufferSizeMax(int val) {
        this.messageBufferSizeMax = val;
    }
    
    public int getMessageBufferSizeMax() {
        return this.messageBufferSizeMax;
    }
    
    public void setMessageBufferSize(int val) {
        this.messageBufferSize = val;
    }
    
    public int getMessageBufferSize() {
        return this.messageBufferSize;
    }
    
    public void setDataMessageSize(int val) {
        this.dataMessageSize = val;
    }
    
    public int getDataMessageSize() {
        return this.dataMessageSize;
    }
    
    @Override
    public Object clone() {
        Configuration that = new Configuration();
        that.boot = this.boot;
        that.bootThreshold = this.bootThreshold;
        that.bootstrapServer = this.bootstrapServer;
        that.hostHooks = new HashSet<ComponentHook>(this.hostHooks);
        that.virtualHooks = new HashSet<VirtualComponentHook>(this.virtualHooks);
        that.ip = this.ip;
        that.port = this.port;
        that.keepAlivePeriod = this.keepAlivePeriod;
        // copying the store is not the smartest thing to do
        //that.db = this.db;
        
        return that;
    }

    /**
     * @return the db
     */
    public Database getDB() {
        return db;
    }

    /**
     * @param db the db to set
     */
    public void setDB(Database db) {
        this.db = db;
    }

    /**
     * @return the bootVNodes
     */
    public int getBootVNodes() {
        return bootVNodes;
    }

    /**
     * @param bootVNodes the bootVNodes to set
     */
    public void setBootVNodes(int bootVNodes) {
        this.bootVNodes = bootVNodes;
    }
}
