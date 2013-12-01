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

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.simulation.command.GetCmd;
import se.sics.caracaldb.simulation.command.PutCmd;
import se.sics.caracaldb.simulation.command.ValidateCmd;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Experiment1 extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(Experiment1.class);
    private static final Random RAND = new Random();

    Positive<Network> net = requires(Network.class);
    Negative<ExperimentPort> expExecutor = provides(ExperimentPort.class);

    // instance
    private ValidationStore store;

    public Experiment1(Experiment1Init init) {
        this.store = new ValidationStore();

        subscribe(getHandler, expExecutor);
        subscribe(validateHandler, expExecutor);
        subscribe(responseHandler, net);
    }

    Handler<CaracalMsg> responseHandler = new Handler<CaracalMsg>() {

        @Override
        public void handle(CaracalMsg event) {
            if (event.op instanceof CaracalResponse) {
                CaracalResponse resp = (CaracalResponse) event.op;
                LOG.debug("Got {} from {}", resp, event.getSource());
                store.response(resp);
            } else {
                LOG.debug("Got an unexpected message {}.", event);
            }
        }
    };
    Handler<GetCmd> getHandler = new Handler<GetCmd>() {
        @Override
        public void handle(GetCmd event) {
            Key k = randomKey(8);
            GetRequest getr = new GetRequest(TimestampIdFactory.get().newId(), k);
            store.request(getr);
            LOG.debug("Sent GET {}", getr);
            trigger(getr, expExecutor);
        }
    };
    Handler<PutCmd> putHandler = new Handler<PutCmd>() {
        @Override
        public void handle(PutCmd event) {
            Key k = randomKey(8);
            Key val = randomKey(32);
            PutRequest putr = new PutRequest(TimestampIdFactory.get().newId(), k, val.getArray());
            store.request(putr);
            LOG.debug("Sent PUT {}", putr);
            trigger(putr, expExecutor);
        }
    };

    Handler<ValidateCmd> validateHandler = new Handler<ValidateCmd>() {
        @Override
        public void handle(ValidateCmd event) {
            SimulationHelper.setValidator(store);
            if (store.isDone()) {
                LOG.info("Placed Validation Store.");
                trigger(new TerminateExperiment(), expExecutor);
            }
        }
    };
    
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
