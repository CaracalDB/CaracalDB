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

import com.larskroll.common.ByteArrayRef;
import io.netty.buffer.ByteBuf;
import java.util.TreeSet;
import java.util.UUID;
import se.sics.caracaldb.flow.FlowManager.Chunk;

/**
 *
 * @author lkroll
 */
public class ByteArrayChunkCollector extends ChunkCollector<ByteArrayRef> {

    /**
     * Don't write into that array!
     */
    public final byte[] data;
    private final TreeSet<Long> receivedChunks = new TreeSet<Long>();
    private final int numberOfChunks;

    public ByteArrayChunkCollector(UUID flowId, int clearId, int bytes) {
        super(flowId, clearId);
        data = new byte[bytes];
        numberOfChunks = (int) Chunk.numberOfChunks(bytes); // works because byte[]
    }

    @Override
    public synchronized boolean isComplete() {
        return receivedChunks.size() == numberOfChunks;
    }

    @Override
    public synchronized TreeSet<Long> getMissingChunks() {
        TreeSet<Long> missing = new TreeSet<Long>();
        if (isComplete()) {
            return missing;
        }
//        long last = -1;
//        for (Long cur : receivedChunks) {
//            for (long i = last + 1; i < cur; i++) {
//                missing.add(i);
//            }
//            last = cur;
//        }
        for (long i = 0; i < numberOfChunks; i++) {
            if (!receivedChunks.contains(i)) {
                missing.add(i);
            }
        }

        return missing;
    }

    @Override
    public synchronized ByteArrayRef readChunk(long chunkNo, int length, ByteBuf buf) {
        int offset = (int)chunkNo * FlowManager.CHUNK_SIZE; // must fit into int because comes from a byte[]
        buf.readBytes(data, (int) offset, length); 
        receivedChunks.add(chunkNo);
        return new ByteArrayRef(offset, length, data);
    }

    @Override
    public ByteArrayRef getResult() {
        return new ByteArrayRef(0, data.length, data);
    }
    
    public static CollectorDescriptor descriptor(int bytes) {
        return new Descriptor(bytes);
    } 
    
    public static class Descriptor implements CollectorDescriptor {

        public final int bytes;
        
        Descriptor(int bytes) {
            this.bytes = bytes;
        }
        
        @Override
        public ChunkCollector create(UUID flowId, int clearId) {
            return new ByteArrayChunkCollector(flowId, clearId, bytes);
        }
        
    }

}
