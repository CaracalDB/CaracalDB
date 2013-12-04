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

import com.google.common.collect.Ordering;
import com.google.common.io.Closer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.NodeStats;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.caracaldb.utils.TopKMap;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Stats {

    private static final Logger LOG = LoggerFactory.getLogger(Stats.class);
    public static final int K = 5;

    private final static Sigar sigar = new Sigar();
    private static double previousCpuUsage = 0.0;

    public static Report collect(Address atHost, Map<Address, NodeStats> nodeStats) {
        Mem mem;
        try {
            mem = sigar.getMem();
        } catch (SigarException se) {
            LOG.error("Sigar Mem Error: ", se);
            return null;
        }
        try {
            double[] cpuLoadAvgs = sigar.getLoadAverage();
            double cpuLoad = cpuLoadAvgs[0];
            if (cpuLoad != Double.NaN) { // This can happen if you request stats too frequently like in simulation
                previousCpuUsage = cpuLoad;
            }
        } catch (SigarException se) {
            LOG.error("Sigar Cpu Error: ", se);
            return null;
        }
        
        TopKMap<Long, Address> topKSize = new TopKMap<Long, Address>(K);
        Comparator<Long> revcomp = Ordering.natural().reverse();
        TopKMap<Long, Address> bottomKSize = new TopKMap<Long, Address>(K, revcomp);
        TopKMap<Long, Address> topKOps = new TopKMap<Long, Address>(K);
        TopKMap<Long, Address> bottomKOps = new TopKMap<Long, Address>(K, revcomp);
        
        for (Entry<Address, NodeStats> e : nodeStats.entrySet()) {
            Address addr = e.getKey();
            NodeStats stats = e.getValue();
            topKSize.put(stats.storeSize, addr);
            bottomKSize.put(stats.storeSize, addr);
            topKOps.put(stats.ops, addr);
            bottomKOps.put(stats.ops, addr);
        }
        
        return new Report(atHost, mem.getUsedPercent(), previousCpuUsage, 
                mapToList(topKSize), mapToList(bottomKSize), 
                mapToList(topKOps), mapToList(bottomKOps));
    }
    
    private static List<Key> mapToList(TopKMap<?, Address> map) {
        ArrayList<Key> list = new ArrayList<Key>(map.size());
        for (Entry<?, Address> e : map.entryList()) {
            Address addr = e.getValue();
            list.add(new Key(addr.getId()));
        }
        return list;
    }

    public static class Report {

        public final Address atHost;
        public final double memoryUsage;
        public final double cpuUsage;
        public final List<Key> topKSize;
        public final List<Key> bottomKSize;
        public final List<Key> topKOps;
        public final List<Key> bottomKOps;

        public Report(Address atHost, double memoryUsage, double cpuUsage, List<Key> topKSize, List<Key> bottomKSize, List<Key> topKOps, List<Key> bottomKOps) {
            this.atHost = atHost;
            this.memoryUsage = memoryUsage;
            this.cpuUsage = cpuUsage;
            this.topKSize = topKSize;
            this.bottomKSize = bottomKSize;
            this.topKOps = topKOps;
            this.bottomKOps = bottomKOps;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Report(");
            sb.append(atHost);
            sb.append(", Mem: ");
            sb.append(memoryUsage);
            sb.append("%, Cpu: ");
            sb.append(cpuUsage);
            sb.append("%)");
            return sb.toString();
        }

        public byte[] serialise() throws IOException {
            Closer closer = Closer.create();
            try {
                ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
                DataOutputStream w = closer.register(new DataOutputStream(baos));

                CustomSerialisers.serialiseAddress(atHost, w);
                w.writeDouble(memoryUsage);
                w.writeDouble(cpuUsage);

                w.writeInt(topKSize.size()); // They better all be the same size -.-

                for (int i = 0; i < topKSize.size(); i++) {
                    CustomSerialisers.serialiseKey(topKSize.get(i), w);
                    CustomSerialisers.serialiseKey(bottomKSize.get(i), w);
                    CustomSerialisers.serialiseKey(topKOps.get(i), w);
                    CustomSerialisers.serialiseKey(bottomKOps.get(i), w);
                }

                w.flush();

                return baos.toByteArray();
            } catch (Throwable e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
            }
        }

        public static Report deserialise(byte[] bytes) throws IOException {
            Closer closer = Closer.create();
            try {
                ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(bytes));
                DataInputStream r = closer.register(new DataInputStream(bais));

                Address atHost = CustomSerialisers.deserialiseAddress(r);
                double memoryUsage = r.readDouble();
                double cpuUsage = r.readDouble();

                int k = r.readInt();

                ArrayList<Key> topKSize = new ArrayList<Key>(k);
                ArrayList<Key> bottomKSize = new ArrayList<Key>(k);
                ArrayList<Key> topKOps = new ArrayList<Key>(k);
                ArrayList<Key> bottomKOps = new ArrayList<Key>(k);

                for (int i = 0; i < k; i++) {
                    topKSize.add(CustomSerialisers.deserialiseKey(r));
                    bottomKSize.add(CustomSerialisers.deserialiseKey(r));
                    topKOps.add(CustomSerialisers.deserialiseKey(r));
                    bottomKSize.add(CustomSerialisers.deserialiseKey(r));
                }

                return new Report(atHost, memoryUsage, cpuUsage, topKSize, bottomKSize, topKOps, bottomKOps);
            } catch (Throwable e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
            }
        }
    }
}
