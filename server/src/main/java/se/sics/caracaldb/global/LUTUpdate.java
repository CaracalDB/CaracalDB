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
package se.sics.caracaldb.global;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.io.Closer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.address.Address;

/**
 *
 * @author sario
 */
public class LUTUpdate implements Maintenance {

    public static final Logger LOG = LoggerFactory.getLogger(LUTUpdate.class);

    private final long previousVersion;
    private final long version;
    private final Action[] diff;

    public LUTUpdate(long previousVersion, long version, Action[] diff) {
        this.previousVersion = previousVersion;
        this.version = version;
        this.diff = diff;
    }

    public void apply(LookupTable lut) {
        lut.versionId = version;
        for (Action a : diff) {
            a.apply(lut);
        }
    }

    public boolean applicable(LookupTable lut) {
        return lut.versionId == previousVersion;
    }

    public long dependsOn() {
        return previousVersion;
    }

    public byte[] serialise() throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            w.writeLong(previousVersion);
            w.writeLong(version);

            w.writeInt(diff.length);
            for (Action a : diff) {
                a.serialise(w);
            }

            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static LUTUpdate deserialise(byte[] bytes) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(bytes));
            DataInputStream r = closer.register(new DataInputStream(bais));

            long previousVersion = r.readLong();
            long version = r.readLong();

            int numActions = r.readInt();
            Action[] actions = new Action[numActions];
            for (int i = 0; i < numActions; i++) {
                actions[i] = Action.deserialise(r);
            }

            return new LUTUpdate(previousVersion, version, actions);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static abstract class Action {

        public static final ImmutableBiMap<Byte, Class<? extends Action>> TYPES;

        static {
            HashBiMap<Byte, Class<? extends Action>> types = HashBiMap.create();

            types.put((byte) 1, PutHost.class);
            types.put((byte) 2, PutReplicationSet.class);

            TYPES = ImmutableBiMap.copyOf(types);
        }

        public byte code() {
            return TYPES.inverse().get(this.getClass());
        }

        public abstract void apply(LookupTable lut);

        public abstract void serialise(DataOutputStream w) throws IOException;

        public abstract void fill(DataInputStream r) throws IOException;

        public static Action deserialise(DataInputStream r) throws IOException, InstantiationException, IllegalAccessException {
            byte type = r.readByte();
            Class<? extends Action> clazz = TYPES.get(type);
            Action a = clazz.newInstance();
            a.fill(r);
            return a;
        }
    }

    public static class PutHost extends Action {

        private int id;
        private Address addr;

        PutHost() {

        }

        public PutHost(int id, Address addr) {
            this.id = id;
            this.addr = addr;
        }

        @Override
        public void apply(LookupTable lut) {
            ArrayList<Address> hosts = lut.hosts();
            while (id >= hosts.size()) {
                hosts.add(null); // just increase the size until it fits (will probably be filled by later ops
            }
            hosts.set(id, addr);
        }

        @Override
        public void serialise(DataOutputStream w) throws IOException {
            w.writeByte(code());
            w.writeInt(id);
            CustomSerialisers.serialiseAddress(addr, w);
        }

        @Override
        public void fill(DataInputStream r) throws IOException {
            id = r.readInt();
            addr = CustomSerialisers.deserialiseAddress(r);
        }

    }

    public static class PutReplicationSet extends Action {

        private int id;
        private int version;
        private Integer[] hosts;

        PutReplicationSet() {

        }

        public PutReplicationSet(int id, int version, Integer[] hosts) {
            this.id = id;
            this.version = version;
            this.hosts = hosts;
        }

        @Override
        public void apply(LookupTable lut) {
            ArrayList<Integer> rsvs = lut.replicationSetVersions();
            ArrayList<Integer[]> rss = lut.replicationSets();
            if (rsvs.size() != rss.size()) {
                LOG.error("Replication Sets and their Versions seems to be out of sync! (" + rss.size() + ", " + rsvs.size() + ")...fixing...");
                // It's fixable at this point...but the more code depends on this, the less likely it is that the fix is actually correct
                while (rsvs.size() > rss.size()) {
                    rss.add(new Integer[0]);
                }
                while (rss.size() > rsvs.size()) {
                    rsvs.add(0);
                }
            }
            // Now that they are definitely of equal size, we just have to check one
            while (id >= rss.size()) {
                rss.add(new Integer[0]); // just increase the size until it fits (will probably be filled by later ops)
                rsvs.add(0);
                //TODO Also add items to the list of reclaimable entries once that code is merged
            }
            rss.set(id, hosts);
            rsvs.set(id, version);
        }

        @Override
        public void serialise(DataOutputStream w) throws IOException {
            w.writeByte(code());
            w.writeInt(id);
            w.writeInt(version);
            w.writeInt(hosts.length);
            for (Integer hostId : hosts) {
                w.writeInt(hostId);
            }
        }

        @Override
        public void fill(DataInputStream r) throws IOException {
            id = r.readInt();
            version = r.readInt();
            int numHosts = r.readInt();
            hosts = new Integer[numHosts];
            for (int i = 0; i < numHosts; i++) {
                hosts[i] = r.readInt();
            }
        }

    }

    public static class PutReplicationGroup extends Action {

        private Key key;
        private int replicationSet;

        PutReplicationGroup() {

        }

        public PutReplicationGroup(Key key, int replicationSet) {
            this.key = key;
            this.replicationSet = replicationSet;
        }

        @Override
        public void apply(LookupTable lut) {
            lut.virtualHostsPut(key, replicationSet);
        }

        @Override
        public void serialise(DataOutputStream w) throws IOException {
            w.writeByte(code());
            CustomSerialisers.serialiseKey(key, w);
            w.writeInt(replicationSet);
        }

        @Override
        public void fill(DataInputStream r) throws IOException {
            key = CustomSerialisers.deserialiseKey(r);
            replicationSet = r.readInt();
        }
    }
}
