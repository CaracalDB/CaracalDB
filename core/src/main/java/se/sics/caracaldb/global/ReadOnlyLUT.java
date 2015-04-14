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

import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.utils.TimestampIdFactory;

/**
 *
 * @author lkroll
 */
public class ReadOnlyLUT {

    static final Logger LOG = LoggerFactory.getLogger(ReadOnlyLUT.class);

    private final Address self;
    private final Random rand;
    private LookupTable lut = null;
    private LUTPart.Collector coll;
    private final TreeMap<Long, LUTUpdate> stalledUpdates = new TreeMap<Long, LUTUpdate>();
    private final TreeMap<UUID, RangeQuery.SeqCollector> collectors = new TreeMap<UUID, RangeQuery.SeqCollector>();
    private final Queue<CaracalMsg> toSend = new LinkedList<CaracalMsg>();

    public ReadOnlyLUT(Address self, Random rand) {
        this.self = self;
        this.rand = rand;
    }

    public boolean collect(LUTPart part) {
        if (lut == null) {
            if (coll == null) {
                coll = part.collector();
            }
            coll.collect(part);
            if (coll.complete()) {
                try {
                    lut = coll.result();
                    applyStalledUpdates();
                } catch (IOException ex) {
                    LOG.warn("Couldn't deserialise LUT. Falling back to random nodes. Reason was: {}", ex);
                }
            }
            return true;
        }
        return false;
    }

    public boolean collect(CaracalResponse resp) {
        RangeQuery.SeqCollector coll = collectors.get(resp.id);
        if (coll != null) {
            coll.processResponse((RangeQuery.Response) resp); // can't have the same id if different type
            if (coll.isDone()) {
                collectors.remove(resp.id);
                try {
                    for (Map.Entry<Key, byte[]> e : coll.getResult().getValue1().entrySet()) {
                        LUTUpdate update = LUTUpdate.deserialise(e.getValue());
                        if (!update.applicable(lut) || (lut == null)) {
                            stalledUpdates.put(update.version, update);
                            askForUpdatesTo(update.version, false);
                            return true;
                        }
                        ROCallbacks chc = new ROCallbacks();
                        update.apply(lut, chc);
                        stalledUpdates.remove(update.version);
                    }
                    applyStalledUpdates();
                } catch (Exception ex) {
                    LOG.error("{}: Error during LUTUpdate deserialisation: \n {}", self, ex);
                }
            }
            return true;
        }
        return false;
    }

    public void handleOutdated(LUTOutdated event) {
        if (lut != null) {
            if (lut.versionId < event.newerlutversion) {
                askForUpdatesTo(event.newerlutversion, true);
            }
        }
    }

    public boolean isReadable() {
        return lut != null;
    }

    public boolean hasMessages() {
        return !toSend.isEmpty();
    }

    public CaracalMsg pollMessages() {
        return toSend.poll();
    }

    private void askForUpdatesTo(long version, boolean incl) {
        try {
            Key startKey = LookupTable.RESERVED_LUTUPDATES.append(new Key(Longs.toByteArray(lut.versionId))).get();
            Key endKey = LookupTable.RESERVED_LUTUPDATES.append(new Key(Longs.toByteArray(version))).get();
            KeyRange range;
            if (incl) {
                range = KeyRange.open(startKey).closed(endKey);
            } else {
                range = KeyRange.open(startKey).open(endKey);
            }
            UUID id = TimestampIdFactory.get().newId();
            RangeQuery.Request r = new RangeQuery.Request(id, range, null, null, null, RangeQuery.Type.SEQUENTIAL);
            Address dest = lut.findDest(startKey, self, rand);
            CaracalMsg msg = new CaracalMsg(self, dest, r);
            toSend.offer(msg);
            collectors.put(id, new RangeQuery.SeqCollector(r));
        } catch (LookupTable.NoResponsibleForKeyException ex) {
            LOG.error("{}: Apparently noone is responsible for the reserved range -.-: {}", self, ex);
        } catch (LookupTable.NoSuchSchemaException ex) {
            LOG.error("{}: Apparently the reserved range doesn't have a schema!!! -.-: {}", self, ex);
        }
    }

    public long version() {
        if (lut != null) {
            return lut.versionId;
        } else {
            return -1;
        }
    }

    public Address findDest(Key k) throws LookupTable.NoResponsibleForKeyException, LookupTable.NoSuchSchemaException {
        if (lut != null) {
            return lut.findDest(k, self, rand);
        }
        return null;
    }

    private void applyStalledUpdates() {
        while (!stalledUpdates.isEmpty()) {
            LUTUpdate update = stalledUpdates.firstEntry().getValue();
            if (update.applicable(lut)) {
                ROCallbacks chc = new ROCallbacks();
                update.apply(lut, chc);
                stalledUpdates.remove(update.version);
            } else {
                return; // since they are linear you can stop at the first non-applicable one
            }
        }
    }

    public String info() {
        if (lut != null) {
            StringBuilder sb = new StringBuilder();
            lut.printFormat(sb);
            return sb.toString();
        } else {
            return "LUT incomplete!";
        }
    }

    public String asJson() {
        if (lut != null) {
            return lut.asJson();
        } else {
            return "";
        }
    }

    public String schemasAsJson() {
        if (lut != null) {
            return LUTJsonProtocol.getSchemas(lut.schemas());
        } else {
            return "";
        }
    }

    public String hostsAsJson() {
        if (lut != null) {
            return LUTJsonProtocol.getHosts(lut.hosts());
        } else {
            return "";
        }
    }

    public class ROCallbacks implements LUTUpdate.Callbacks {

        @Override
        public Address getAddress() {
            return null;
        }

        @Override
        public Integer getAddressId() {
            return -1;
        }

        @Override
        public void killVNode(Key k) {
            // ignore
        }

        @Override
        public void startVNode(Key k) {
            // ignore
        }

        @Override
        public void reconf(Key key, View v, int quorum, KeyRange range) {
            // ignore
        }

    }
}
