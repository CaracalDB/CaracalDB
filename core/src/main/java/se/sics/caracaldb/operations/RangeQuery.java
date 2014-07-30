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
package se.sics.caracaldb.operations;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.store.ActionFactory;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeAction;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.store.TransformationFilter;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class RangeQuery {

    public static class Request extends CaracalOp {

        public KeyRange subRange;
        public final KeyRange initRange;
        public final Type execType;
        public final Limit.LimitTracker limitTracker;
        public final TransformationFilter transFilter;
        public final RangeAction action;

        public Request(Request req, KeyRange newRange) {
            super(req.id);
            this.initRange = newRange;
            this.subRange = newRange;
            this.limitTracker = req.limitTracker;
            this.transFilter = req.transFilter;
            this.action = req.action;
            this.execType = Type.SEQUENTIAL;
        }

        public Request(UUID id, KeyRange range, Limit.LimitTracker limitTracker, TransformationFilter transFilter, RangeAction action, Type execType) {
            super(id);
            this.initRange = range;
            this.subRange = range;
            this.limitTracker = limitTracker;
            this.transFilter = transFilter;
            this.action = action;
            this.execType = execType;
        }

        Request(UUID id, KeyRange subRange, KeyRange initRange, Limit.LimitTracker limitTracker, TransformationFilter transFilter, RangeAction action, Type execType) {
            super(id);
            this.initRange = initRange;
            this.subRange = subRange;
            this.limitTracker = limitTracker;
            this.transFilter = transFilter;
            this.action = action;
            this.execType = execType;
        }

        @Override
        public String toString() {
            return "RangeQuery Request(" + id + ", " + initRange.toString() + ", " + subRange.toString() + ")";
        }

        @Override
        public boolean affectedBy(CaracalOp op) {
            if (op instanceof PutRequest) {
                PutRequest put = (PutRequest) op;
                return subRange.contains(put.key);
            }
            if (op instanceof RangeQuery.Request) {
                RangeQuery.Request rqr = (RangeQuery.Request) op;
                if (!(rqr.action instanceof ActionFactory.Noop)) {
                    return KeyRange.overlap(subRange, rqr.subRange);
                }
            }
            if (op instanceof MultiOpRequest) {
                MultiOpRequest mor = (MultiOpRequest) op;
                return mor.isInRange(subRange);
            }
            return false;
        }

        public Request subRange(KeyRange newSubRange) {
            return new Request(id, newSubRange, initRange, limitTracker, transFilter, action, execType);
        }
    }

    public enum Type {

        SEQUENTIAL;
    }

    public static class Response extends CaracalResponse {

        public final KeyRange subRange;
        public final KeyRange initRange;
        public final SortedMap<Key, byte[]> data;
        public final boolean readLimit;

        public Response(Request req, RangeResp event) {
            super(event.getId(), ResponseCode.SUCCESS);
            this.subRange = req.subRange;
            this.initRange = req.initRange;
            this.data = event.result;
            this.readLimit = event.readLimit;
        }

        public Response(UUID id, KeyRange subRange, KeyRange initRange, SortedMap<Key, byte[]> data, boolean readLimit) {
            super(id, ResponseCode.SUCCESS);
            this.subRange = subRange;
            this.initRange = initRange;
            this.data = data;
            this.readLimit = readLimit;
        }

        public Response(ResponseCode code, Request req) {
            super(req.id, code);
            this.subRange = req.subRange;
            this.initRange = req.initRange;
            this.data = null;
            this.readLimit = false;
        }
        
        Response(UUID id, ResponseCode code, KeyRange subRange, KeyRange initRange, SortedMap<Key, byte[]> data, boolean readLimit) {
            super(id, code);
            this.subRange = subRange;
            this.initRange = initRange;
            this.data = data;
            this.readLimit = readLimit;
        }

        @Override
        public String toString() {
            String str = "RangeQuery Response(" + id + ", ";
            str += code.name() + ")";
            return str;
        }
    }
    
    public static class InternalResponse extends CaracalResponse {
        public final RangeResp resp;
        
        public InternalResponse(RangeResp resp) {
            super(resp.getId(), ResponseCode.SUCCESS);
            this.resp = resp;
        }
        
        public Response finalize(Request req) {
            return new Response(req, resp);
        }
        
        @Override
        public String toString() {
            String str = "RangeQuery InternalResponse(" + id + ", ";
            str += code.name() + ")";
            return str;
        }
    }

    public static class SeqCollector {

        public final Request req;
        private final TreeMap<Key, byte[]> results;
        private boolean done;
        private final TreeMap<Key, KeyRange> pendingSubRanges;
        private KeyRange coveredRange;

        public SeqCollector(Request req) {
            this.req = req;
            results = new TreeMap<Key, byte[]>();
            pendingSubRanges = new TreeMap<Key, KeyRange>();
            pendingSubRanges.put(req.subRange.begin, req.subRange);
        }

        public boolean isDone() {
            return done;
        }

        public Pair<KeyRange, TreeMap<Key, byte[]>> getResult() {
            return Pair.with(coveredRange, results);
        }

        public RangeResponse getResponse() {
            Pair<KeyRange, TreeMap<Key, byte[]>> res = getResult();
            return new RangeResponse(req.id, req.initRange, ResponseCode.SUCCESS, res.getValue0(), res.getValue1());
        }

        /**
         * @param resp expected to be SUCCESS
         */
        public void processResponse(Response resp) {
            if (done) {
                return;
            }
            if (!resp.id.equals(req.id)) {
                return;
            }
            KeyRange respRange = resp.subRange;
            KeyRange primaryRange = removePrimaryRange(respRange.begin);
            addRestRange(primaryRange, respRange);
            checkReadLimit(resp.readLimit, respRange.end);
            results.putAll(resp.data);
            if (pendingSubRanges.isEmpty()) {
                done = true;
                if (coveredRange == null) {
                    coveredRange = req.subRange;
                }
            }
        }

        private KeyRange removePrimaryRange(Key key) {
            Entry<Key, KeyRange> entry = pendingSubRanges.floorEntry(key);
            if (entry == null) {
                entry = pendingSubRanges.firstEntry();
            }
            pendingSubRanges.remove(entry.getKey());
            return entry.getValue();
        }

        private void addRestRange(KeyRange primaryRange, KeyRange subRange) {
            if (!primaryRange.equals(subRange)) {
                if (!subRange.end.equals(primaryRange.end)) {
                    KeyRange rightRange = KeyRange.closed(subRange.end).endFrom(primaryRange);
                    pendingSubRanges.put(rightRange.begin, rightRange);
                }
                if (!subRange.begin.equals(primaryRange.begin)) {
                    KeyRange leftRange = KeyRange.closed(primaryRange.begin).open(subRange.begin);
                    pendingSubRanges.put(leftRange.begin, leftRange);
                }
            }

        }

        private void checkReadLimit(boolean readLimit, Key end) {
            if (readLimit) {
                SortedMap<Key, KeyRange> rest = pendingSubRanges.tailMap(end);
                if (rest.isEmpty()) {
                    coveredRange = req.subRange;
                } else {
                    coveredRange = KeyRange.startFrom(req.subRange).open(rest.firstKey());
                }
                for (Key key : rest.keySet()) {
                    pendingSubRanges.remove(key);
                }
            }
        }
    }
}
