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
import io.netty.buffer.ByteBuf;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import se.sics.caracaldb.utils.ByteArrayRef;
import se.sics.caracaldb.flow.FlowManager.Chunk;

/**
 *
 * @author lkroll
 */
public class ChunkCollector {

    public static final ConcurrentMap<ClearFlowId, ChunkCollector> collectors = new ConcurrentSkipListMap<>();

    public final ClearFlowId id;
    /**
     * Don't write into that array!
     */
    public final byte[] data;
    private final TreeSet<Integer> receivedChunks = new TreeSet<>();
    private final int numberOfChunks;

    public ChunkCollector(UUID flowId, int clearId, int bytes) {
        id = new ClearFlowId(flowId, clearId);
        data = new byte[bytes];
        numberOfChunks = Chunk.numberOfChunks(bytes);
    }

    public synchronized boolean isComplete() {
        return receivedChunks.size() == numberOfChunks;
    }

    public synchronized TreeSet<Integer> getMissingChunks() {
        TreeSet<Integer> missing = new TreeSet<>();
        if (isComplete()) {
            return missing;
        }
        int last = -1;
        for (Integer cur : receivedChunks) {
            for (int i = last + 1; i < cur; i++) {
                missing.add(i);
            }
            last = cur;
        }

        return missing;
    }

    public synchronized ByteArrayRef readChunk(int chunkNo, int length, ByteBuf buf) {
        int offset = chunkNo * FlowManager.CHUNK_SIZE;
        buf.readBytes(data, offset, length);
        receivedChunks.add(chunkNo);
        return new ByteArrayRef(offset, length, data);
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
