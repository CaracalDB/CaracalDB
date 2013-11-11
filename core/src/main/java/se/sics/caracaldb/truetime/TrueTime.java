/*
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package se.sics.caracaldb.truetime;

import java.util.concurrent.atomic.AtomicLong;
import se.sics.kompics.ComponentDefinition;

/**
 *
 * @author sario
 */
public class TrueTime extends ComponentDefinition {
    public static final double MAX_CLOCK_DRIFT = 200.0/1000.0/1000.0; //200µs/s expressed in ms/ms
    private static AtomicLong maxError = new AtomicLong(5000); // 5s is quite a lot...
    private static AtomicLong lastSync = new AtomicLong(System.currentTimeMillis());
    
    public static TrueTimeInterval now() {
        long localNow = System.currentTimeMillis();
        long sinceLastSync = Math.abs(localNow - lastSync.get()); // Could theoretically be negative as local clocks are not necessarily monotonic
        long drift = (long) Math.ceil(MAX_CLOCK_DRIFT*sinceLastSync);
        long error = maxError.get();
        long earliest = localNow-error-drift;
        long latest = localNow+error+drift;
        return new TrueTimeInterval(earliest, latest);
    }
    
    //TODO implement some distributed clock synchronization algorithm (like Reachback Firefly Algorithm (RFA))
}
