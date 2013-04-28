/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface ComponentHook {
    public void setUp(HostSharedComponents shared, ComponentProxy parent);
    public void tearDown(HostSharedComponents shared, ComponentProxy parent);
}
