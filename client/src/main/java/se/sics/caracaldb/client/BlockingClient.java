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
package se.sics.caracaldb.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.global.Schema;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.MultiOpRequest;
import se.sics.caracaldb.operations.MultiOpResponse;
import se.sics.caracaldb.operations.OpUtil;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.RangeResponse;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.store.ActionFactory;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.Limit.LimitTracker;
import se.sics.caracaldb.store.TFFactory;
import se.sics.caracaldb.utils.TimestampIdFactory;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BlockingClient {

    // static
    private static final Logger LOG = LoggerFactory.getLogger(BlockingClient.class);
    private static final long TIMEOUT = 1;
    private static final TimeUnit TIMEUNIT = TimeUnit.SECONDS;
    // instance
    private final BlockingQueue<CaracalResponse> responseQueue;

    private final ClientWorker worker;

    BlockingClient(BlockingQueue<CaracalResponse> responseQueue, ClientWorker worker) {
        this.responseQueue = responseQueue;
        this.worker = worker;
    }

    public boolean test() {
        return worker.test();
    }

    public ResponseCode put(String schema, Key key, byte[] value) {
        Key k = worker.resolveSchema(schema, key);
        if (k == null) {
            LOG.info("Could not resolve schema name for {}:{}", schema, key);
            return ResponseCode.NOT_READY;
        }
        LOG.debug("Putting on {}", k);
        PutRequest req = new PutRequest(TimestampIdFactory.get().newId(), k, value);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return ResponseCode.CLIENT_TIMEOUT;
            }
            return resp.code;
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return ResponseCode.CLIENT_TIMEOUT;
        }
    }

    public GetResponse get(String schema, Key key) {
        Key k = worker.resolveSchema(schema, key);
        if (k == null) {
            LOG.info("Could not resolve schema name for {}:{}", schema, key);
            return new GetResponse(null, key, null, ResponseCode.NOT_READY);
        }
        LOG.debug("Getting for {}", k);
        UUID id = TimestampIdFactory.get().newId();
        GetRequest req = new GetRequest(id, k);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new GetResponse(id, k, null, ResponseCode.CLIENT_TIMEOUT);
            }
            if (resp instanceof GetResponse) {
                return (GetResponse) resp;
            }
            return new GetResponse(id, k, null, ResponseCode.UNSUPPORTED_OP);
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new GetResponse(id, k, null, ResponseCode.CLIENT_TIMEOUT);
        }
    }

    public RangeResponse rangeRequest(String schema, KeyRange range) {
        return rangeRequest(schema, range, Limit.noLimit());
    }

    public RangeResponse rangeRequest(String schema, KeyRange range, LimitTracker limit) {
        KeyRange r = worker.resolveSchema(schema, range);
        if (r == null) {
            LOG.info("Could not resolve schema name for {}:{}", schema, range);
            return new RangeResponse(null, r, ResponseCode.NOT_READY, null, null);
        }
        LOG.debug("RangeRequest for {}", r);
        UUID id = TimestampIdFactory.get().newId();
        RangeQuery.Request req = new RangeQuery.Request(id, r, limit, TFFactory.noTF(), ActionFactory.noop(), RangeQuery.Type.SEQUENTIAL);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new RangeResponse(id, r, ResponseCode.CLIENT_TIMEOUT, null, null);
            }
            if (resp instanceof RangeResponse) {
                return (RangeResponse) resp;
            }
            return new RangeResponse(id, r, ResponseCode.UNSUPPORTED_OP, null, null);
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new RangeResponse(id, r, ResponseCode.CLIENT_TIMEOUT, null, null);
        }
    }

    public RangeResponse append(String schema, Key key, byte[] newData) {
        Key k = worker.resolveSchema(schema, key);
        if (k == null) {
            LOG.info("Could not resolve schema name for {}:{}", schema, k);
            return new RangeResponse(null, KeyRange.key(key), ResponseCode.NOT_READY, null, null);
        }
        LOG.debug("Append for {}", k);
        UUID id = TimestampIdFactory.get().newId();
        RangeQuery.Request req = OpUtil.append(id, k, newData);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new RangeResponse(id, req.initRange, ResponseCode.CLIENT_TIMEOUT, null, null);
            }
            if (resp instanceof RangeResponse) {
                return (RangeResponse) resp;
            }
            return new RangeResponse(id, req.initRange, ResponseCode.UNSUPPORTED_OP, null, null);
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new RangeResponse(id, req.initRange, ResponseCode.CLIENT_TIMEOUT, null, null);
        }
    }

    public MultiOpResponse cas(String schema, Key key, byte[] oldValue, byte[] newValue) {
        Key k = worker.resolveSchema(schema, key);
        if (k == null) {
            LOG.info("Could not resolve schema name for {}:{}", schema, key);
            return new MultiOpResponse(null, ResponseCode.NOT_READY, false);
        }
        LOG.debug("Putting (CAS) on {}", k);
        MultiOpRequest req = OpUtil.cas(TimestampIdFactory.get().newId(), k, oldValue, newValue);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new MultiOpResponse(req.id, ResponseCode.CLIENT_TIMEOUT, false);
            }
            if (resp instanceof MultiOpResponse) {
                return (MultiOpResponse) resp;
            }
            return new MultiOpResponse(req.id, ResponseCode.UNSUPPORTED_OP, false);
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new MultiOpResponse(req.id, ResponseCode.CLIENT_TIMEOUT, false);
        }
    }

    public MultiOpResponse putIfAbsent(String schema, Key key, byte[] value) {
        Key k = worker.resolveSchema(schema, key);
        if (k == null) {
            LOG.info("Could not resolve schema name for {}:{}", schema, key);
            return new MultiOpResponse(null, ResponseCode.NOT_READY, false);
        }
        LOG.debug("Putting (if absent) on {}", k);
        MultiOpRequest req = OpUtil.putIfAbsent(TimestampIdFactory.get().newId(), k, value);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new MultiOpResponse(req.id, ResponseCode.CLIENT_TIMEOUT, false);
            }
            if (resp instanceof MultiOpResponse) {
                return (MultiOpResponse) resp;
            }
            return new MultiOpResponse(req.id, ResponseCode.UNSUPPORTED_OP, false);
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new MultiOpResponse(req.id, ResponseCode.CLIENT_TIMEOUT, false);
        }
    }

    public Future<Schema.Response> createSchema(String name, ImmutableMap<String, String> metaData) {
        CreateSchema cs = new CreateSchema(name, metaData);
        worker.triggerOnSelf(cs);
        return cs.future;
    }

    public Future<Schema.Response> dropSchema(String name) {
        DropSchema ds = new DropSchema(name);
        worker.triggerOnSelf(ds);
        return ds.future;
    }
    
    public ImmutableSortedSet<String> listSchemas() {
        return worker.listSchemas();
    }
    
    public String getSchemaInfo(String schemaName) {
        return worker.getSchemaInfo(schemaName);
    }
}
