/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import com.google.common.io.Closer;
import java.io.IOException;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.persistence.StoreIterator;
import se.sics.caracaldb.store.Limit.LimitTracker;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class RangeReq extends StorageRequest {

    public final KeyRange range;
    private final TransformationFilter transFilter;
    private final LimitTracker limit;

    public RangeReq(KeyRange range, LimitTracker limit, TransformationFilter transFilter) {
        this.range = range;
        if (limit != null) {
            this.limit = limit;
        } else {
            this.limit = Limit.noLimit();
        }
        if (transFilter != null) {
            this.transFilter = transFilter;
        } else {
            this.transFilter = TFFactory.noTF();
        }
    }

    @Override
    public Response execute(Persistence store) throws IOException {
        TreeMap<Key, byte[]> results = new TreeMap<Key, byte[]>();

        Closer closer = Closer.create();
        try {
            byte[] begin = range.begin.getArray();
            for (StoreIterator it = closer.register(store.iterator(begin)); it.hasNext(); it.next()) {
                byte[] key = it.peekKey();
                if (range.contains(key)) {
                    Pair<Boolean, byte[]> res = transFilter.execute(it.peekValue());
                    if (!res.getValue0()) {
                        if (limit.read(res.getValue1())) {
                            results.put(new Key(key), res.getValue1());
                            if (!limit.canRead()) {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    //special case (a,b) and key is a
                    if (Key.compare(begin, key) != 0) {
                        break; // reached end of range
                    }
                }
            }
        } catch (Throwable e) {
            closer.rethrow(e);
        } finally {
            closer.close();
        }
        return new RangeResp(this, results, !limit.canRead());
    }
}
