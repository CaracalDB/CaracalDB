/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Lars Kroll <lkr@lars-kroll.com>
 */
public class Configuration {
    
    private InetAddress ip;
    private int port;
    private Set<ComponentHook> hostHooks = new HashSet<ComponentHook>();
    private Set<VirtualComponentHook> virtualHooks = new HashSet<VirtualComponentHook>();
    
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
}
