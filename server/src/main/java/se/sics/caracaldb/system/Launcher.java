/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.kompics.Kompics;

/**
 *
 * @author Lars Kroll <lkr@lars-kroll.com>
 */
public abstract class Launcher {
    private static Configuration currentConfig;
    
    public static void main(String[] args) {
        
    }
    
    public static void reset() {
        currentConfig = new Configuration();
    }
    
    public static Configuration getCurrentConfig() {
        if (currentConfig == null) {
            reset();
        }
        return currentConfig;
        
    }
    
    public static void start() {
        Kompics.createAndStart(HostManager.class, Runtime.getRuntime().availableProcessors(), 20); // Yes 20 is totally arbitrary
    }
    
    public static void stop() {
        Kompics.shutdown();
    }
}
