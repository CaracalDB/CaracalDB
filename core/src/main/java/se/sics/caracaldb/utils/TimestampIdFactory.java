/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class TimestampIdFactory implements IdFactory {
    
    private static TimestampIdFactory instance;

    private final byte[] address;
    
    @Override
    public synchronized long newId() {
        byte[] millis = Longs.toByteArray(System.currentTimeMillis());
        byte[] nanos = Longs.toByteArray(System.nanoTime());
        return Longs.fromBytes(address[0], address[1], address[2], address[3], 
                millis[2], millis[1], millis[0], nanos[0]);
    }
    
    private TimestampIdFactory(Address adr) {
        this.address = Ints.toByteArray(adr.hashCode());
    }
    
    public static synchronized TimestampIdFactory get() {
        if (instance == null) {
            throw new RuntimeException("IdFactory has not yet been instatiated!");
        }
        return instance;
    }
    
    public static synchronized void init(Address adr) {
        instance = new TimestampIdFactory(adr);
    }
    
}
