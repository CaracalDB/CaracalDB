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
package se.sics.caracaldb.utils;

import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import se.sics.caracaldb.Key;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class CustomSerialisers {
    /*
     * Custom Address serialisation to save some space
     */

    public static void serialiseAddress(Address addr, DataOutputStream w) throws IOException {
        if (addr == null) {
            w.writeInt(0); //simply put four 0 bytes since 0.0.0.0 is not a valid host ip
            return;
        }
        w.write(addr.getIp().getAddress());
        // Write ports as 2 bytes instead of 4
        byte[] portBytes = Ints.toByteArray(addr.getPort());
        w.writeByte(portBytes[2]);
        w.writeByte(portBytes[3]);
        serialiseKey(addr.getId(), w);

    }

    public static byte[] serialiseAddress(Address addr) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            serialiseAddress(addr, w);
            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static Address deserialiseAddress(DataInputStream r) throws IOException {
        byte[] ipBytes = new byte[4];
        if (r.read(ipBytes) != 4) {
            throw new IOException("Incomplete dataset!");
        }
        if ((ipBytes[0] == 0) && (ipBytes[1] == 0) && (ipBytes[2] == 0) && (ipBytes[3] == 0)) {
            return null; // IP 0.0.0.0 is not valid but null Address encoding
        }
        InetAddress ip = InetAddress.getByAddress(ipBytes);
        byte portUpper = r.readByte();
        byte portLower = r.readByte();
        int port = Ints.fromBytes((byte) 0, (byte) 0, portUpper, portLower);
        Key id = deserialiseKey(r);
        return new Address(ip, port, id.getArray());
    }

    public static void serialiseKey(Key key, DataOutputStream w) throws IOException {
        if (key instanceof Key.Inf) {
            w.writeBoolean(true);
        } else {
            w.writeBoolean(false);
        }
        byte[] id = key.getArray();
        if (id != null) {
            if (id.length <= Key.BYTE_KEY_SIZE) {
                w.writeBoolean(true);
                w.writeByte(id.length);
            } else {
                w.writeBoolean(false);
                w.writeInt(id.length);
            }
            w.write(id);
        } else {
            w.writeBoolean(true);
            w.writeByte(0);
        }
    }

    public static void serialiseKey(byte[] key, DataOutputStream w) throws IOException {
        serialiseKey(new Key(key), w);
    }

    public static byte[] serialiseKey(Key key) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            serialiseKey(key, w);
            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static byte[] serialiseKey(byte[] key) throws IOException {
        return serialiseKey(new Key(key));
    }

    public static Key deserialiseKey(DataInputStream r) throws IOException {
        if (r.readBoolean()) {
            return Key.INF;
        }
        boolean isByteLength = r.readBoolean();
        int keySize;
        if (isByteLength) {
            keySize = UnsignedBytes.toInt(r.readByte());
        } else {
            keySize = r.readInt();
        }
        byte[] id;
        if (keySize == 0) {
            id = null;
        } else {
            id = new byte[keySize];
            if (r.read(id) != keySize) {
                throw new IOException("Incomplete dataset!");
            }
        }
        return new Key(id);
    }
}
