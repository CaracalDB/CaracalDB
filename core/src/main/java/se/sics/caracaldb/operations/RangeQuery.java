/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

import java.util.SortedMap;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.store.TransformationFilter;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class RangeQuery {

    public static class Request extends CaracalOp {
        
        public KeyRange subRange;
        public final KeyRange initRange;
        public final Limit.LimitTracker limitTracker;
        public final TransformationFilter transFilter;
        public final Type execType;

        public Request(Request req, KeyRange newRange) {
            super(req.id);
            this.initRange = newRange;
            this.subRange = newRange;
            this.limitTracker = req.limitTracker;
            this.transFilter = req.transFilter;
            this.execType = Type.SEQUENTIAL;
        }

        public Request(long id, KeyRange range, Limit.LimitTracker limitTracker, TransformationFilter transFilter, Type execType) {
            super(id);
            this.initRange = range;
            this.subRange = range;
            this.limitTracker = limitTracker;
            this.transFilter = transFilter;
            this.execType = execType;
        }
       
        private Request(long id, KeyRange subRange, KeyRange initRange, Limit.LimitTracker limitTracker, TransformationFilter transFilter, Type execType) {
            super(id);
            this.initRange = initRange;
            this.subRange = subRange;
            this.limitTracker = limitTracker;
            this.transFilter = transFilter;
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
            return false;
        }
        
        public Request subRange(KeyRange newSubRange) {
            return new Request(id, newSubRange, initRange, limitTracker, transFilter, execType);
        }
    }
    
    public enum Type {
        PARALLEL, SEQUENTIAL;
    }

    public static class Response extends CaracalResponse {
        public Request req;
        public final SortedMap<Key, byte[]> data;
        public final boolean readLimit;

        public Response(RangeResp event) {
            super(event.getId(), ResponseCode.SUCCESS);
            this.data = event.result;
            this.readLimit = event.readLimit;
        }

        /**
         * used when operation is successful
         */
        public Response(long id, KeyRange range, SortedMap<Key, byte[]> data, boolean readLimit) {
            super(id, ResponseCode.SUCCESS);
            this.data = data;
            this.readLimit = readLimit;
        }

        /**
         * used when the operation is unsuccessful
         */
        public Response(long id, ResponseCode code, KeyRange range) {
            super(id, code);
            this.data = null;
            this.readLimit = false;
        }

        public void setReq(Request req) {
            this.req = req;
        }
        
        @Override
        public String toString() {
            String str = "RangeQuery Response(" + id + ", ";
            str += code.name() + ")";
            return str;
        }
    }
}
