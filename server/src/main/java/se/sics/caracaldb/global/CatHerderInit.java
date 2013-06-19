/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import java.util.Set;
import se.sics.caracaldb.bootstrap.Bootstrapped;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class CatHerderInit extends Init<CatHerder> {
    
    public final Bootstrapped bootEvent;
    public final Address self;
    
    public CatHerderInit(Bootstrapped bootr, Address self) {
        bootEvent = bootr;
        this.self = self;
    }

}
