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

import com.larskroll.common.PartialFileRef;
import com.larskroll.common.RAFileRef;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

/**
 *
 * @author lkroll
 */
public class FileChunkCollector extends ChunkCollector<PartialFileRef> {

    private final TreeSet<Long> receivedChunks = new TreeSet<Long>();
    private final long numberOfChunks;
    private final RandomAccessFile raf;
    private final RAFileRef rafr;

    public FileChunkCollector(UUID flowId, int clearId, long bytes) {
        super(flowId, clearId);
        try {
            numberOfChunks = FlowManager.Chunk.numberOfChunks(bytes);
            File tmp = File.createTempFile("transfer", ".data");
            raf = new RandomAccessFile(tmp, "rws");
            raf.setLength(bytes);
            rafr = new RAFileRef(tmp, raf);
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(ex); // this really shouldn't happen unless we ran out of tmp space or something
        } catch (IOException ex) {
            throw new RuntimeException(ex); // this really shouldn't happen unless we ran out of tmp space or something
        }
    }

    @Override
    public synchronized boolean isComplete() {
        return receivedChunks.size() == numberOfChunks;
    }

    @Override
    public synchronized SortedSet<Long> getMissingChunks() {
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
    public synchronized PartialFileRef readChunk(long chunkNo, int length, ByteBuf buf) {
        receivedChunks.add(chunkNo);
        try {
            long offset = chunkNo * FlowManager.CHUNK_SIZE;
            raf.seek(offset);
            buf.readBytes(raf.getChannel(), length);

            return new PartialFileRef(offset, length, rafr);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public synchronized PartialFileRef getResult() {
        return new PartialFileRef(0, rafr.size(), rafr);
    }

    public static CollectorDescriptor descriptor(long bytes) {
        return new Descriptor(bytes);
    }

    public static class Descriptor implements CollectorDescriptor {

        public final long bytes;

        Descriptor(long bytes) {
            this.bytes = bytes;
        }

        @Override
        public ChunkCollector create(UUID flowId, int clearId) {
            return new FileChunkCollector(flowId, clearId, bytes);
        }

    }
}
