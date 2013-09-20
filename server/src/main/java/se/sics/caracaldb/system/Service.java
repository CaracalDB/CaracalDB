/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import com.google.common.collect.ComparisonChain;
import se.sics.kompics.Component;
import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Service implements Comparable<Service> {
    public final String name;
    public final Class<? extends PortType> type;
    public final Component provider;
    
    public Service(String name, Class<? extends PortType> type, Component provider) {
        this.name = name;
        this.type = type;
        this.provider = provider;
    }

    @Override
    public int compareTo(Service that) {
        return ComparisonChain.start()
         .compare(this.type.getCanonicalName(), that.type.getCanonicalName())
         .compare(this.name, that.name)
         .result();
    }
}
