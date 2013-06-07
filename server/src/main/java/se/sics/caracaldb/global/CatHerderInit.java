/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import java.util.Set;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class CatHerderInit extends Init {
    
    public final LookupTable lut;
    
    public CatHerderInit(LookupTable lut) {
        this.lut = lut;
    }

}
