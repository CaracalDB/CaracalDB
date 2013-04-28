/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface VirtualComponentHook {
    public void setUp(VirtualSharedComponents shared, ComponentProxy parent);
    public void tearDown(VirtualSharedComponents shared, ComponentProxy parent);
}
