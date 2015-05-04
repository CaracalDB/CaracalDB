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
package se.sics.caracaldb.replication.linearisable;

import java.util.List;
import org.javatuples.Pair;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.replication.log.Value;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface OperationsLog extends Iterable<Value> {

    /**
     * Inserts a decided operation into the log and returns a list of operations
     * which are newly committable.
     * <p>
     * If the log has no gaps after inserting, the returned list will only
     * contain op. If the new op creates a gap, the returned list will be empty.
     * If the new op closes a gap, the returned list will contain the op and all
     * consecutive gapless operations in the log.
     * <p>
     * @param pos
     * @param op
     * @return
     */
    public List<Pair<Long, Value>> insert(long pos, Value op);

    /**
     * Drops the log entries up to and including pos.
     * <p>
     * @param pos
     */
    public void prune(long pos);

    public void clear();

    /**
     *
     * @return true if the log is completely empty
     */
    public boolean isEmpty();


    /**
     * Get a list of committable operations since lastSnapshot.
     * <p>
     * @param lastSnapshot
     * @return
     */
    public Pair<Long, List<CaracalOp>> getSnapshotDiff(long lastSnapshot);

    /**
     * Return the number of operations in the log.
     * <p>
     * @return
     */
    public Integer size();
}
