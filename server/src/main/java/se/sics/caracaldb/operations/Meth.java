/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Meth extends Init<MethCat> {
    public final Address self;
    public final KeyRange responsibility;
    public final View view;
    

    public Meth(Address self, KeyRange responsibility, View view) {
        this.self = self;
        this.responsibility = responsibility;
        this.view = view;
    }
}
