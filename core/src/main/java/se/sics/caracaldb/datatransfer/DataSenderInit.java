/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import java.util.Map;
import se.sics.caracaldb.KeyRange;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DataSenderInit extends Init<DataSender> {
    
    public final long id;
    public final KeyRange range;
    public final Address self;
    public final Address destination;
    public final long retryTime;
    public final int maxSize;
    public final Map<String, Object> metadata;
    
    public DataSenderInit(long id, KeyRange range, Address self, Address dest, long retryTime, int maxSize, Map<String, Object> metadata) {
        this.id = id;
        this.range = range;
        this.self = self;
        this.destination = dest;
        this.retryTime = retryTime;
        this.maxSize = maxSize;
        this.metadata = metadata;
    }
}
