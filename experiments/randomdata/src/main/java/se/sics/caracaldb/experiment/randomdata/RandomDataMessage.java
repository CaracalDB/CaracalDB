/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.experiment.randomdata;

import java.util.UUID;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.BaseDataMessage;
import se.sics.kompics.network.Transport;

/**
 *
 * @author lkroll
 */
public class RandomDataMessage extends BaseDataMessage {
    
    public final UUID id;
    public final byte[] data;
    public final long arrivalTime; // to be set in the deserialiser

    public RandomDataMessage(Address src, Address dst, Transport proto, UUID id, byte[] data) {
        super(src, dst, proto);
        this.id = id;
        this.data = data;
        this.arrivalTime = -1;
    }
    
    public RandomDataMessage(Address src, Address dst, Transport proto, UUID id, byte[] data, long arrivalTime) {
        super(src, dst, proto);
        this.id = id;
        this.data = data;
        this.arrivalTime = arrivalTime;
    }
    
}
