/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.kompics.virtual.networkmodel;

import java.util.Random;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class UniformRandomModel implements NetworkModel {
    
    private final long min;
    private final long max;
    private final long diff;
    private final Random rand;
    
    public UniformRandomModel(long min, long max) {
        this(min, max, new Random(1));
    }
    
    public UniformRandomModel(long min, long max, Random rand) {
        this.min = min;
        this.max = max;
        this.diff = max - min;
        this.rand = rand;
    }

    @Override
    public long getLatencyMs(Message message) {
        return min + (long)Math.floor(rand.nextDouble() * diff);
    }
    
    
}
