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
package se.sics.caracaldb.simulation;

import com.esotericsoftware.minlog.Log;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.simulation.ValidationStore2.Validator;
import se.sics.caracaldb.simulation.command.OpCmd;
import se.sics.caracaldb.simulation.command.PutCmd;
import se.sics.caracaldb.simulation.command.RQCmd;
import se.sics.caracaldb.simulation.command.TerminateCmd;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.TFFactory;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class Experiment2 extends ComponentDefinition {

    private static final Random RAND = new Random();
    private static final Logger LOG = LoggerFactory.getLogger(Experiment2.class);

    Negative<Experiment2Port> expExecutor = provides(Experiment2Port.class);
    Positive<Network> net = requires(Network.class);

    private final ValidationStore2 resultValidator;
    private final List<OpCmd> pendingOps;
    private Validator validator;
    private boolean idle;

    public Experiment2(Experiment2Init init) {
        this.resultValidator = init.store;
        this.pendingOps = new LinkedList<OpCmd>();
        this.idle = true;
        this.validator = null;

        subscribe(operationHandler, expExecutor);
        subscribe(terminateHandler, expExecutor);
        subscribe(responseHandler, net);
    }

    Handler<OpCmd> operationHandler = new Handler<OpCmd>() {

        @Override
        public void handle(OpCmd op) {
            if (idle) {
                doOp(op);
            } else {
                pendingOps.add(op);
            }
        }
    };
    Handler<TerminateCmd> terminateHandler = new Handler<TerminateCmd>() {

        @Override
        public void handle(TerminateCmd event) {
            if (idle) {
                LOG.warn("need more time to finish ops");
            } else {
                resultValidator.endExperiment();
            }
        }
    };
    Handler<CaracalMsg> responseHandler = new Handler<CaracalMsg>() {

        @Override
        public void handle(CaracalMsg resp) {
            try {
                idle = validator.validateAndContinue((CaracalResponse)resp.op);
            } catch (ValidationStore2.ValidatorException ex) {
                LOG.error("validator error {}", ex);
                System.exit(1);
            }

            if (idle && !pendingOps.isEmpty()) {
                doOp(pendingOps.remove(0));
            }
        }
    };

    private void doOp(OpCmd op) {
        if (op instanceof PutCmd) {
            idle = false;
            long id = TimestampIdFactory.get().newId();
            Key k = randomKey(8);
            Key val = randomKey(32);
            PutRequest put = new PutRequest(id, k, val.getArray());
            validator = resultValidator.startOp(put);
            LOG.debug("put request {}, key:{}", id, k);
            trigger(put, expExecutor);
        } else if (op instanceof RQCmd) {
            idle = false;
            long id = TimestampIdFactory.get().newId();
            KeyRange range = KeyRange.closed(randomKey(8)).open(randomKey(8));
            RangeQuery.Request rq = new RangeQuery.Request(id, range, Limit.noLimit(), TFFactory.noTF(), RangeQuery.Type.SEQUENTIAL);
            validator = resultValidator.startOp(rq);
            LOG.debug("rangequery request {}, range:{}", id, range);
            trigger(rq, expExecutor);
        }
    }

    private Key randomKey(int size) {
        int s = size;
        if (size == -1) {
            s = Math.abs(RAND.nextInt(1000));
        }
        byte[] bytes = new byte[s];
        RAND.nextBytes(bytes);
        // don't write in the 00 XX... key range
        // it's reserved
        if (bytes[0] == 0) {
            bytes[0] = 1;
        }
        return new Key(bytes);
    }
}
