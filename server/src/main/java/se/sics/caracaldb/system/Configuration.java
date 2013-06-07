/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
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
    private long keepAlivePeriod = 1000;
    
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
        
        return that;
    }
}
