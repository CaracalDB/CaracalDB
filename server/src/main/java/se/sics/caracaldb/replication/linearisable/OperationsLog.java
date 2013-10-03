/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
     * If the log has no gaps after inserting, the returned list will only contain op.
     * If the new op creates a gap, the returned list will be empty.
     * If the new op closes a gap, the returned list will contain the op and 
     *  all consecutive gapless operations in the log.
     * 
     * @param pos
     * @param op
     * @return 
     */
    public List<Pair<Long,Value>> insert(long pos, Value op);
    /**
     * Drops the log entries up to and including pos.
     * 
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
     * Compiles a list of operations that change the result of op
     * since the lastSnapshot and if op were inserted at pos.
     * <p>
     * 
     * To define which operations affect which other operations see
     * {@link CaracalOp#affectsResult(CaracalOp)}.
     * This method only considers gap free prefixes.
     * 
     * @param pos
     * @param op
     * @param lastSnapshot
     * @return 
     */
    public List<CaracalOp> getApplicableForOp(long pos, CaracalOp op, long lastSnapshot);
    /**
     * Get a list of committable operations since lastSnapshot.
     * 
     * @param lastSnapshot
     * @return 
     */
    public Pair<Long, List<CaracalOp>> getSnapshotDiff(long lastSnapshot);
}
