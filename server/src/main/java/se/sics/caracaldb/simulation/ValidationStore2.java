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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.PutResponse;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.persistence.Database;
import se.sics.caracaldb.persistence.StoreIterator;
import se.sics.caracaldb.persistence.memory.InMemoryDB;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.system.Launcher;
import com.larskroll.common.ByteArrayRef;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ValidationStore2 {

    private final Database store;
    private static final Logger LOG = LoggerFactory.getLogger(ValidationStore2.class);

    private boolean end = false;

    public ValidationStore2() {
        this.store = new InMemoryDB(Launcher.getConfig().core());
    }

    public Validator startOp(CaracalOp op) {
        if (op instanceof GetRequest) {
            GetRequest get = (GetRequest) op;
            return new GetValidator(get, store.get(get.key.getArray()));
        } else if (op instanceof PutRequest) {
            PutRequest put = (PutRequest) op;
            store.put(put.key.getArray(), put.data, 0);
            return new PutValidator(put);
        } else if (op instanceof RangeQuery.Request) {
            RangeQuery.Request rq = (RangeQuery.Request) op;
            return new RangeQueryValidator(rq, getExpectedRangeResult(rq));
        } else {
            return null;
        }
    }

    public void endExperiment() {
        this.end = true;
    }

    public boolean experimentEnded() {
        return end;
    }

    private NavigableMap<Key, ByteArrayRef> getExpectedRangeResult(RangeQuery.Request rq) {
        TreeMap<Key, ByteArrayRef> expectedResult = new TreeMap<Key, ByteArrayRef>();
        Limit.LimitTracker lt = rq.limitTracker.doClone();
        StoreIterator it = store.iterator(rq.initRange.begin.getArray());
        while (it.hasNext()) {
            Key key = new Key(it.peekKey());
            if (!rq.subRange.contains(key)) {
                if (key.equals(rq.subRange.begin)) {
                    continue;
                }
                break;
            }
            if (lt.canRead()) {
                Pair<Boolean, ByteArrayRef> result = rq.transFilter.execute(it.peekValue());
                if (result.getValue0()) {
                    if (lt.read(result.getValue1())) {
                        expectedResult.put(key, result.getValue1());
                    }
                    if (!lt.canRead()) {
                        break;
                    }
                }
                it.next();
            } else {
                break;
            }
        }
        return expectedResult;
    }

    public static interface Validator {

        /**
         * @param resp
         * @return true if operation finished successfully, false if the
         * operation is not done or throw and exception if the operation failed
         * @throws
         * se.sics.caracaldb.simulation.ValidationStore2.ValidatorException
         */
        boolean validateAndContinue(CaracalResponse resp) throws ValidatorException;
    }

    static class GetValidator implements Validator {

        private final GetRequest req;
        private final ByteArrayRef expectedResult;

        GetValidator(GetRequest req, ByteArrayRef expectedResult) {
            this.req = req;
            this.expectedResult = expectedResult;
        }

        @Override
        public boolean validateAndContinue(CaracalResponse resp) throws ValidatorException {
            LOG.trace("Got response {}", resp);
            if (!resp.id.equals(req.id)) {
                LOG.warn("was not expecting resp {}", resp);
                return false;
            } else if (resp instanceof GetResponse) {
                GetResponse getResp = (GetResponse) resp;
                if (!getResp.code.equals(ResponseCode.SUCCESS)) {
                    LOG.error("operation {} failed with resp {}", new Object[]{req, resp.code});
                    throw new ValidatorException();
                } else if (expectedResult.equals(getResp.data)) {
                    LOG.info("Got response {} success", resp);
                    return true;
                } else {
                    LOG.error("operation {} returned wrong value", req);
                    throw new ValidatorException();
                }
            } else {
                LOG.error("operation {} received wrong response type {}", new Object[]{req, resp});
                throw new ValidatorException();
            }
        }
    }

    static class PutValidator implements Validator {

        private final PutRequest req;

        PutValidator(PutRequest req) {
            this.req = req;
        }

        @Override
        public boolean validateAndContinue(CaracalResponse resp) throws ValidatorException {
            LOG.trace("Got response {}", resp);
            if (!resp.id.equals(req.id)) {
                LOG.warn("was not expecting resp {}", resp);
                return false;
            } else if (resp instanceof PutResponse) {
                PutResponse putResp = (PutResponse) resp;
                if (!putResp.code.equals(ResponseCode.SUCCESS)) {
                    LOG.error("operation {} failed with resp {}", new Object[]{req, resp.code});
                    throw new ValidatorException();
                } else {
                    LOG.info("Got response {}", resp);
                    return true;
                }
            } else {
                LOG.error("operation {} received wrong response type {}", new Object[]{req, resp});
                throw new ValidatorException();
            }
        }
    }

    static class RangeQueryValidator implements Validator {

        private final RangeQuery.Request req;
        private final RangeQuery.SeqCollector col;
        private final NavigableMap<Key, ByteArrayRef> expectedResult;

        RangeQueryValidator(RangeQuery.Request req, NavigableMap<Key, ByteArrayRef> expectedResult) {
            this.req = req;
            this.col = new RangeQuery.SeqCollector(req);
            this.expectedResult = expectedResult;
        }

        @Override
        public boolean validateAndContinue(CaracalResponse resp) throws ValidatorException {
            LOG.trace("Got response {}", resp);
            if (!resp.id.equals(req.id)) {
                LOG.warn("was not expecting resp {}", resp);
                return false;
            } else if (resp instanceof RangeQuery.Response) {
                RangeQuery.Response rqResp = (RangeQuery.Response) resp;
                if (!rqResp.code.equals(ResponseCode.SUCCESS)) {
                    LOG.error("operation {} failed with resp {}", new Object[]{req, resp.code});
                    throw new ValidatorException();
                }
                LOG.info("operation {} successfull with {} results", new Object[]{req, rqResp.data.size()});
                col.processResponse(rqResp);
                if (col.isDone()) {
                    TreeMap<Key, byte[]> result = col.getResult().getValue1();
                    if (result.size() != expectedResult.size()) {
                        LOG.error("operation {} failed with different number of returned elements, expected:{} found:{}", req, expectedResult.size(), result.size());
                        Iterator<Key> it1 = expectedResult.keySet().iterator();
                        Iterator<Key> it2 = result.keySet().iterator();
                        while(it1.hasNext() && it2.hasNext()) {
                            if(it1.next().equals(it2.next())) {
                                continue;
                            }
                            LOG.debug("wrong ordered keys");
                            throw new ValidatorException();
                        }
                        TreeSet<Key> aux = new TreeSet<Key>();
                        for (Key k : expectedResult.keySet()) {
                            if(!result.containsKey(k)) {
                                aux.add(k);
                            }
                        }
                        LOG.error("last key:{} first missing key{}", result.lastKey(), aux.first());
                        throw new ValidatorException();
                    }
                    for (Entry<Key, byte[]> e : result.entrySet()) {
                        ByteArrayRef expectedData = expectedResult.get(e.getKey());
                        if (!expectedData.equals(e.getValue())) {
                            LOG.error("operation {} returned wrong value for key:{}", req, e.getKey());
                            throw new ValidatorException();
                        }
                    }
                    return true;
                } else {
                    return false;
                }
            } else {
                LOG.debug("operation {} received wrong response type {}", new Object[]{req, resp});
                throw new ValidatorException();
            }
        }
    }

    public static class ValidatorException extends Exception {
    }
}
