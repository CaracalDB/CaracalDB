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

import com.google.common.base.Optional;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.View;
import se.sics.caracaldb.replication.linearisable.ViewChange;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 *
 * @author sario
 */
public class LUTUpdate implements Maintenance {

    public static final Logger LOG = LoggerFactory.getLogger(LUTUpdate.class);

    private final long previousVersion;
    public final long version;
    private final Action[] diff;

    private ByteBuf serializationCache = null;

    public LUTUpdate(long previousVersion, long version, Action[] diff) {
        this.previousVersion = previousVersion;
        this.version = version;
        this.diff = diff;
    }

    public void apply(LookupTable lut, Callbacks call) {
        lut.versionId = version;
        for (Action a : diff) {
            a.apply(lut, call);
        }
    }

    public boolean applicable(LookupTable lut) {
        return lut.versionId == previousVersion;
    }

    public long dependsOn() {
        return previousVersion;
    }
    
    ImmutableSet<Address> joiners() {
        ImmutableSet.Builder<Address> joiners = ImmutableSet.builder();
        for (Action a : diff) {
            if (a instanceof PutHost) {
                PutHost ph = (PutHost) a;
                if (ph.addr != null) {
                    joiners.add(ph.addr);
                }
            }
        }
        return joiners.build();
    }

    public byte[] serialise() {
        if (serializationCache == null) {
            ByteBuf buf = Unpooled.buffer();
            serialise(buf);
            buf.release();
        }

        byte[] data = new byte[serializationCache.readableBytes()];
        serializationCache.getBytes(0, data);
        return data;
    }

    public void serialise(ByteBuf buf) {
        if (serializationCache == null) {
            serializationCache = Unpooled.buffer();
            serializationCache.writeLong(previousVersion);
            serializationCache.writeLong(version);

            serializationCache.writeInt(diff.length);
            for (Action a : diff) {
                a.serialise(serializationCache);
            }
        }
        buf.writeBytes(serializationCache, 0, serializationCache.readableBytes());
    }

    public static LUTUpdate deserialise(byte[] bytes) throws InstantiationException, IllegalAccessException {
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);

        LUTUpdate lutu = deserialise(buf);

        buf.release();

        return lutu;
    }

    public static LUTUpdate deserialise(ByteBuf buf) throws InstantiationException, IllegalAccessException {
        long previousVersion = buf.readLong();
        long version = buf.readLong();

        int numActions = buf.readInt();
        Action[] actions = new Action[numActions];
        for (int i = 0; i < numActions; i++) {
            actions[i] = Action.deserialise(buf);
        }

        return new LUTUpdate(previousVersion, version, actions);
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

        public abstract void apply(LookupTable lut, Callbacks call);

        public abstract void serialise(ByteBuf buf);

        public abstract void fill(ByteBuf buf);

        public static Action deserialise(ByteBuf buf) throws InstantiationException, IllegalAccessException {
            byte type = buf.readByte();
            Class<? extends Action> clazz = TYPES.get(type);
            Action a = clazz.newInstance();
            a.fill(buf);
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
        public void apply(LookupTable lut, Callbacks call) {
            ArrayList<Address> hosts = lut.hosts();
            while (id >= hosts.size()) {
                hosts.add(null); // just increase the size until it fits (will probably be filled by later ops)
            }
            hosts.set(id, addr);
        }

        @Override
        public void serialise(ByteBuf buf) {
            buf.writeByte(code());
            buf.writeInt(id);
            SpecialSerializers.AddressSerializer.INSTANCE.toBinary(addr, buf);
        }

        @Override
        public void fill(ByteBuf buf) {
            id = buf.readInt();
            addr = (Address) SpecialSerializers.AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
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
        public void apply(LookupTable lut, Callbacks call) {
            int posOld = LookupTable.positionInSet(lut.replicationSets().get(id), call.getAddressId());
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
            int posNew = LookupTable.positionInSet(hosts, call.getAddressId());
            if ((posNew >= 0) && (posOld < 0)) { // new in the set
                Set<Key> vNodes = lut.getVirtualNodesFor(id);
                for (Key k : vNodes) {
                    call.startVNode(k);
                }
            }
            if ((posNew < 0) && (posOld >= 0)) { // removed from set
                Set<Key> vNodes = lut.getVirtualNodesFor(id);
                for (Key k : vNodes) {
                    call.killVNode(k);
                }
            }
            if ((posNew >= 0) && (posOld >= 0)) { // no change for me, but set membership changed
                Set<Key> vNodes = lut.getVirtualNodesFor(id);
                for (Key k : vNodes) {
                    View v = new View(ImmutableSortedSet.copyOf(lut.getHosts(id)), version);
                    call.reconf(k, new ViewChange(v, v.members.size() / 2 + 1, lut.getResponsibility(k)));
                }
            }
        }

        @Override
        public void serialise(ByteBuf buf) {
            buf.writeByte(code());
            buf.writeInt(id);
            buf.writeInt(version);
            buf.writeInt(hosts.length);
            for (Integer hostId : hosts) {
                buf.writeInt(hostId);
            }
        }

        @Override
        public void fill(ByteBuf buf) {
            id = buf.readInt();
            version = buf.readInt();
            int numHosts = buf.readInt();
            hosts = new Integer[numHosts];
            for (int i = 0; i < numHosts; i++) {
                hosts[i] = buf.readInt();
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
        public void apply(LookupTable lut, Callbacks call) {
            Integer[] oldHosts = lut.getResponsibleIds(key);
            int posOld = LookupTable.positionInSet(oldHosts, call.getAddressId());
            lut.virtualHostsPut(key, replicationSet);
            Integer[] newHosts = lut.replicationSets().get(replicationSet);
            int posNew = LookupTable.positionInSet(newHosts, call.getAddressId());
            if ((posNew >= 0) && (posOld < 0)) { // new in the set                
                call.startVNode(key);
            }
            if ((posNew < 0) && (posOld >= 0)) { // removed from set                
                call.killVNode(key);
            }
            if ((posNew >= 0) && (posOld >= 0)) { // no change for me, but set membership changed
                View v = new View(ImmutableSortedSet.copyOf(lut.getResponsibles(key)), lut.replicationSetVersions().get(replicationSet));
                call.reconf(key, new ViewChange(v, v.members.size() / 2 + 1, lut.getResponsibility(key)));
            }
        }

        @Override
        public void serialise(ByteBuf buf) {
            buf.writeByte(code());
            CustomSerialisers.serialiseKey(key, buf);
            buf.writeInt(replicationSet);
        }

        @Override
        public void fill(ByteBuf buf) {
            key = CustomSerialisers.deserialiseKey(buf);
            replicationSet = buf.readInt();
        }
    }

    public static interface Callbacks {

        public Address getAddress();

        public Integer getAddressId();

        public void killVNode(Key k);

        public void startVNode(Key k);

        public void reconf(Key key, ViewChange change);
    }
}
