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
