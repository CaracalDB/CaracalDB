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
package se.sics.caracaldb.global;

import com.larskroll.common.J6;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.BaseMessage;
import se.sics.caracaldb.MessageRegistrator;
import se.sics.caracaldb.MessageSerializationUtil.MessageFields;
import se.sics.kompics.network.Transport;

/**
 *
 * @author lkroll
 */
public class LUTPart extends BaseMessage implements LookupMessage {

    public static final int MAX_PART_SIZE = MessageRegistrator.STREAM_MAX_MSG_SIZE;

    public final int offset;
    public final int totalsize;
    final byte[] data;

    LUTPart(Address src, Address dst, Address orig, int offset, int totalsize, byte[] data) {
        super(src, dst, orig, Transport.UDT);
        this.offset = offset;
        this.totalsize = totalsize;
        this.data = data;
    }

    public static List<LUTPart> split(Address src, Address dst, byte[] data) {
        List<LUTPart> parts = new LinkedList<LUTPart>();
        for (int i = 0; i < LUTPart.numberOfPieces(data.length); i++) {
            int offset = LUTPart.piece2Offset(i);
            parts.add(new LUTPart(src, dst, src, offset, data.length, data));
        }
        return parts;
    }
    
    public int length() {
        return Math.min(MAX_PART_SIZE, data.length - offset);
    }

    public Collector collector() {
        return new Collector(totalsize);
    }

    void serialiseContent(ByteBuf buf) {
        buf.writeInt(offset);
        buf.writeInt(totalsize);
        buf.writeInt(length());
        buf.writeBytes(data, offset, length());
    }

    static LUTPart deserialiseContent(ByteBuf buf, MessageFields fields) {
        int offset = buf.readInt();
        int totalsize = buf.readInt();
        int dataL = buf.readInt();
        byte[] data = new byte[dataL];
        buf.readBytes(data);
        return new LUTPart(fields.src, fields.dst, fields.orig, offset, totalsize, data);
    }

    static int numberOfPieces(int totalsize) {
        return J6.roundUp(totalsize, MAX_PART_SIZE);
    }

    static int offset2Piece(int offset) {
        return J6.roundUp(offset, MAX_PART_SIZE);
    }

    static int piece2Offset(int piece) {
        return piece * MAX_PART_SIZE;
    }

    public static class Collector {

        private byte[] data;
        private SortedSet<Integer> pieces = new TreeSet<Integer>();
        private int totalpieces;

        Collector(int totalsize) {
            data = new byte[totalsize];
            totalpieces = LUTPart.numberOfPieces(totalsize);
        }

        public void collect(LUTPart part) {
            pieces.add(LUTPart.offset2Piece(part.offset));
            System.arraycopy(part.data, 0, data, part.offset, part.length());
        }

        public boolean complete() {
            return pieces.size() == totalpieces;
        }

        public int firstMissingPiece() {
            if (pieces.isEmpty()) {
                return 0;
            }
            Iterator<Integer> it = pieces.iterator();
            int next = it.next();
            if (next != 0) { // we might miss the first piece
                return 0;
            }
            next++;
            while (it.hasNext()) {
                int cur = it.next();
                if (cur != next) {
                    break;
                }
                next++;
            }
            return next;
        }

        public LookupTable result() throws IOException {
            return LookupTable.deserialise(data);
        }
    }

}
