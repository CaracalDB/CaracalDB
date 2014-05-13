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
package se.sics.caracaldb.system;

import com.google.common.util.concurrent.AtomicDouble;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author lkroll
 */
public class CaracalStats implements CaracalStatsMBean {

    final AtomicLong storeSize = new AtomicLong(0);
    final AtomicLong numberOfKeys = new AtomicLong(0);
    final AtomicLong averageOpS = new AtomicLong(0);
    final AtomicInteger numberOfVNodes = new AtomicInteger(0);
    final AtomicDouble memoryUsage = new AtomicDouble(0.0);
    final AtomicDouble cpuUsage = new AtomicDouble(0.0);

    @Override
    public long getStoreSize() {
        return storeSize.get();
    }

    @Override
    public long getNumberOfKeys() {
        return numberOfKeys.get();
    }

    @Override
    public long getAverageOpS() {
        return averageOpS.get();
    }

    @Override
    public double getMemoryUsage() {
        return memoryUsage.get();
    }

    @Override
    public double getCpuUsage() {
        return cpuUsage.get();
    }

    @Override
    public int getNumberOfVNodes() {
        return numberOfVNodes.get();
    }
}
