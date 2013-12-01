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

import com.google.common.io.Closer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Stats {
    
    private static final Logger LOG = LoggerFactory.getLogger(Stats.class);

    private final static Sigar sigar = new Sigar();
    private static double previousCpuUsage = 0.0;
    
    public static Report collect(Address atHost) {
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
        return new Report(atHost, mem.getUsedPercent(), previousCpuUsage);
    }

    public static class Report {

        public final Address atHost;
        public final double memoryUsage;
        public final double cpuUsage;

        public Report(Address atHost, double memoryUsage, double cpuUsage) {
            this.atHost = atHost;
            this.memoryUsage = memoryUsage;
            this.cpuUsage = cpuUsage;
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


            return new Report(atHost, memoryUsage, cpuUsage);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
    }
}
