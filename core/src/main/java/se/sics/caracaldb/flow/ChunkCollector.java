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
package se.sics.caracaldb.flow;

import com.google.common.collect.ComparisonChain;
import com.larskroll.common.DataRef;
import io.netty.buffer.ByteBuf;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author lkroll
 * @param <Ref>
 */
public abstract class ChunkCollector<Ref extends DataRef> {

    private static final ConcurrentMap<ClearFlowId, ChunkCollector> collectors = new ConcurrentSkipListMap<ClearFlowId, ChunkCollector>();

    public final ClearFlowId id;

    public ChunkCollector(UUID flowId, int clearId) {
        id = new ClearFlowId(flowId, clearId);
    }
    
    public abstract boolean isComplete();
    
    public abstract SortedSet<Long> getMissingChunks();
    
    public abstract Ref readChunk(long chunkNo, int length, ByteBuf buf);
    
    public abstract Ref getResult();
    
    public static ChunkCollector get(ClearFlowId id) {
        return collectors.get(id);
    }
    
    public static ChunkCollector getOrCreate(ClearFlowId id, CollectorDescriptor cd) {
        ChunkCollector coll = ChunkCollector.collectors.get(id);
        if (coll == null) {
            ChunkCollector newColl = cd.create(id.flowId, id.clearId);
            coll = ChunkCollector.collectors.putIfAbsent(id, newColl);
            if (coll == null) { // there was no value put in since the get
                coll = newColl;
            }
        }
        return coll;
    }

    public static ChunkCollector remove(ClearFlowId id) {
        return collectors.remove(id);
    }
    
    public static ClearFlowId getIdFor(UUID flowId, int clearId) {
        return new ClearFlowId(flowId, clearId);
    }

    public static class ClearFlowId implements Comparable<ClearFlowId> {

        public final UUID flowId;
        public final int clearId;

        public ClearFlowId(UUID flowId, int clearId) {
            this.flowId = flowId;
            this.clearId = clearId;
        }

        @Override
        public int compareTo(ClearFlowId that) {
            return ComparisonChain.start()
                    .compare(this.flowId, that.flowId)
                    .compare(this.clearId, that.clearId)
                    .result();
        }

    }
}
