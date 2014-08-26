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

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.NodeStats;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.caracaldb.utils.ExtremeKMap;
import se.sics.caracaldb.utils.TopKMap;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Stats {

    private static final Logger LOG = LoggerFactory.getLogger(Stats.class);
    public static final int K = 5;

    private final static Sigar sigar = new Sigar();
    private static double previousCpuUsage = 0.0;
    private final static CaracalStats mbean = new CaracalStats();

    static {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("se.sics.caracaldb.system:type=CaracalStats");
            mbs.registerMBean(mbean, name);
        } catch (Exception ex) { // without java8 multi catch it's easier to just catch whatever
            LOG.warn("Couldn't subscribe JMX object: {}", ex);
        }
    }

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
        ExtremeKMap<Long, Address> xKSize = new ExtremeKMap<Long, Address>(K);
        ExtremeKMap<Long, Address> xKOps = new ExtremeKMap<Long, Address>(K);
        long totalStoreSize = 0;
        long totalNumKeys = 0;
        long totalOpS = 0;

        for (Entry<Address, NodeStats> e : nodeStats.entrySet()) {
            Address addr = e.getKey();
            NodeStats stats = e.getValue();
            totalStoreSize += stats.storeSize;
            totalNumKeys += stats.storeNumberOfKeys;
            totalOpS += stats.ops;
            xKSize.put(stats.storeSize, addr);
            xKOps.put(stats.ops, addr);
        }

        double memUsage = mem.getUsedPercent();
        int numVN = nodeStats.size();

        mbean.cpuUsage.set(previousCpuUsage);
        mbean.memoryUsage.set(memUsage);
        mbean.storeSize.set(totalStoreSize);
        mbean.numberOfVNodes.set(numVN);
        mbean.numberOfKeys.set(totalNumKeys);
        try {
            mbean.averageOpS.set(Stats.floorDiv(totalOpS, numVN));
            return new Report(atHost, memUsage, previousCpuUsage, numVN,
                    Stats.floorDiv(totalOpS, numVN), Stats.floorDiv(totalStoreSize, numVN),
                    mapToList(xKSize.top()), mapToList(xKSize.bottom()),
                    mapToList(xKOps.top()), mapToList(xKOps.bottom()));
        } catch (ArithmeticException ex) {
            mbean.averageOpS.set(0);
            return new Report(atHost, memUsage, previousCpuUsage, 0,
                    0, 0, mapToList(xKSize.top()), mapToList(xKSize.bottom()),
                    mapToList(xKOps.top()), mapToList(xKOps.bottom()));
        }
    }

    private static List<Key> mapToList(TopKMap<?, Address> map) {
        ArrayList<Key> list = new ArrayList<Key>(map.size());
        for (Entry<?, Address> e : map.entryList()) {
            Address addr = e.getValue();
            list.add(new Key(addr.getId()));
        }
        return list;
    }

    public static long floorDiv(long a, int b) {
        double aD = (double) a;
        double bD = (double) b;
        double cD = aD/bD;
        return (long) Math.floor(cD);
    }

    public static class Report {

        public final Address atHost;
        public final double memoryUsage;
        public final double cpuUsage;
        public final int numberOfVNodes;
        public final long averageOpS;
        public final long averageSize;
        public final List<Key> topKSize;
        public final List<Key> bottomKSize;
        public final List<Key> topKOps;
        public final List<Key> bottomKOps;

        public Report(Address atHost, double memoryUsage, double cpuUsage,
                int numberOfVNodes, long averageOpS, long averageSize,
                List<Key> topKSize, List<Key> bottomKSize, List<Key> topKOps, List<Key> bottomKOps) {
            this.atHost = atHost;
            this.memoryUsage = memoryUsage;
            this.cpuUsage = cpuUsage;
            this.numberOfVNodes = numberOfVNodes;
            this.averageOpS = averageOpS;
            this.averageSize = averageSize;
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
            sb.append("%, VNodes: ");
            sb.append(numberOfVNodes);
            sb.append(", avg. Op/s: ");
            sb.append(averageOpS);
            sb.append(" per VNode, avg. Size: ");
            sb.append(averageSize);
            sb.append(" of a VNode, Top-K (Size): [");
            for (Key k : topKSize) {
                sb.append(k);
                sb.append(", ");
            }
            sb.append("]");
            sb.append(", Bottom-K (Size): [");
            for (Key k : bottomKSize) {
                sb.append(k);
                sb.append(", ");
            }
            sb.append("]");
            sb.append(", Top-K (Op/s): [");
            for (Key k : topKOps) {
                sb.append(k);
                sb.append(", ");
            }
            sb.append("]");
            sb.append(", Bottom-K (Op/s): [");
            for (Key k : bottomKOps) {
                sb.append(k);
                sb.append(", ");
            }
            sb.append("]");
            sb.append(")");
            return sb.toString();
        }

        public byte[] serialise() throws IOException {
            ByteBuf buf = Unpooled.buffer();

            SpecialSerializers.AddressSerializer.INSTANCE.toBinary(atHost, buf);
            buf.writeDouble(memoryUsage);
            buf.writeDouble(cpuUsage);
            buf.writeInt(numberOfVNodes);
            buf.writeLong(averageOpS);
            buf.writeLong(averageSize);

            buf.writeInt(topKSize.size()); // They better all be the same size -.-

            for (int i = 0; i < topKSize.size(); i++) {
                CustomSerialisers.serialiseKey(topKSize.get(i), buf);
                CustomSerialisers.serialiseKey(bottomKSize.get(i), buf);
                CustomSerialisers.serialiseKey(topKOps.get(i), buf);
                CustomSerialisers.serialiseKey(bottomKOps.get(i), buf);
            }

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            buf.release();

            return data;
        }

        public static Report deserialise(byte[] bytes) throws IOException {
            ByteBuf buf = Unpooled.wrappedBuffer(bytes);

            Address atHost = (Address) SpecialSerializers.AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            double memoryUsage = buf.readDouble();
            double cpuUsage = buf.readDouble();
            int numberOfVNodes = buf.readInt();
            long averageOpS = buf.readLong();
            long averageSize = buf.readLong();

            int k = buf.readInt();

            ArrayList<Key> topKSize = new ArrayList<Key>(k);
            ArrayList<Key> bottomKSize = new ArrayList<Key>(k);
            ArrayList<Key> topKOps = new ArrayList<Key>(k);
            ArrayList<Key> bottomKOps = new ArrayList<Key>(k);

            for (int i = 0; i < k; i++) {
                topKSize.add(CustomSerialisers.deserialiseKey(buf));
                bottomKSize.add(CustomSerialisers.deserialiseKey(buf));
                topKOps.add(CustomSerialisers.deserialiseKey(buf));
                bottomKSize.add(CustomSerialisers.deserialiseKey(buf));
            }

            return new Report(atHost, memoryUsage, cpuUsage, numberOfVNodes, averageOpS, averageSize, topKSize, bottomKSize, topKOps, bottomKOps);
        }
    }
}
