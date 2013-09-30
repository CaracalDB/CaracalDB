/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.log;

import se.sics.kompics.PortType;

/**
 * Interface of a ReplicatedLog.
 * <p>
 * Implementations should provide the following semantics:
 * <br />
 * 1) Any value that is decided was proposed
 * <br />
 * 2) A value proposed by a correct node is eventually decided for at least one instance
 * <br />
 * 3) If any node decides a value v in instance i then all correct nodes decide v in i
 * <br />
 * 4) If a Reconfiguration from view v to view v' is decided in instance i, 
 *      the value for i+1 must be decided on all correct nodes of v'
 * <br />
 * 5) If a client requests pruning the log to position i, all log entries
 *      j <= i are dropped.
 * <p>
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ReplicatedLog extends PortType {{
    request(Propose.class);
    request(Prune.class);
    indication(Decide.class);
}}
