/*
 * This file is part of the CaracalDB distributed storage system.
 * replace(" +$", "", "r")}
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
package se.sics.caracaldb.simulation;

import com.esotericsoftware.minlog.Log;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.PutResponse;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.store.PersistentStore;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ValidationStore2 {

    private TreeMap<Key, byte[]> store;
    private Validator opValidator;
    private static final Logger LOG = LoggerFactory.getLogger(ValidationStore2.class);

    public void startOp(CaracalOp op) {
        if (op instanceof GetRequest) {
            opValidator = new GetValidator((GetRequest) op);
        }
    }

    static interface Validator {

        boolean validateAndContinue(CaracalResponse resp, NavigableMap<Key, byte[]> store) throws ValidatorException;
    }

    static class GetValidator implements Validator {

        private GetRequest req;

        GetValidator(GetRequest req) {
            this.req = req;
        }

        @Override
        public boolean validateAndContinue(CaracalResponse resp, NavigableMap<Key, byte[]> store) throws ValidatorException {
            if (resp.id != req.id) {
                LOG.debug("was not expecting resp {}", resp);
                return false;
            } else if (resp instanceof GetResponse) {
                GetResponse getResp = (GetResponse) resp;
                if (!getResp.code.equals(ResponseCode.SUCCESS)) {
                    LOG.debug("operation {} failed with resp {}", new Object[]{req, resp.code});
                    throw new ValidatorException();
                } else if (Arrays.equals(store.get(req.key), getResp.data)) {
                    return true;
                } else {
                    LOG.debug("operation {} returned wrong value", req);
                    throw new ValidatorException();
                }
            } else {
                LOG.debug("operation {} received wrong response type {}", new Object[]{req, resp});
                throw new ValidatorException();
            }
        }
    }
    
    static class PutValidator implements Validator {

        private PutRequest req;

        PutValidator(PutRequest req) {
            this.req = req;
        }

        @Override
        public boolean validateAndContinue(CaracalResponse resp, NavigableMap<Key, byte[]> store) throws ValidatorException {
            if (resp.id != req.id) {
                LOG.debug("was not expecting resp {}", resp);
                return false;
            } else if (resp instanceof PutResponse) {
                PutResponse putResp = (PutResponse) resp;
                if (!putResp.code.equals(ResponseCode.SUCCESS)) {
                    LOG.debug("operation {} failed with resp {}", new Object[]{req, resp.code});
                    throw new ValidatorException();
                } else {
                    return true;
                }
            } else {
                LOG.debug("operation {} received wrong response type {}", new Object[]{req, resp});
                throw new ValidatorException();
            }
        }
    }
    
//    static class RangeQueryValidator implements Validator {
//
//        private RangeQuery.Request req;
//        private Set<KeyRange> subRanges;
//        private TreeMap<
//
//        RangeQueryValidator(RangeQuery.Request req) {
//            this.req = req;
//            subRanges = new HashSet<KeyRange>();
//        }
//
//        @Override
//        public boolean validateAndContinue(CaracalResponse resp, NavigableMap<Key, byte[]> store) throws ValidatorException {
//            if (resp.id != req.id) {
//                LOG.debug("was not expecting resp {}", resp);
//                return false;
//            } else if (resp instanceof RangeQuery.Response) {
//                RangeQuery.Response rqResp = (RangeQuery.Response) resp;
//                if (!rqResp.code.equals(ResponseCode.SUCCESS)) {
//                    LOG.debug("operation {} failed with resp {}", new Object[]{req, resp.code});
//                    throw new ValidatorException();
//                } else {
//                    
//                }
//            } else {
//                LOG.debug("operation {} received wrong response type {}", new Object[]{req, resp});
//                throw new ValidatorException();
//            }
//        }
//    }

    public static class ValidatorException extends Exception {
    }
}
