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

import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedInts;
import java.util.Arrays;

/**
 * See also {se.sics.kompics.address.IdUtils}.
 * @author lkroll
 */
public abstract class ByteArrayFormatter {

    public static String toHexString(byte[] bytes) {
        if (bytes == null) {
            return "(null)";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            String bStr = UnsignedBytes.toString(bytes[i], 16);
            if (bStr.length() == 1) {
                sb.append('0');
            }
            sb.append(bStr.toUpperCase());
        }
        return sb.toString();
    }

    public static byte[] fromHexString(String str) {
        if (str.equals("(null)")) {
            return null;
        }
        if (str.equals("")) {
            return new byte[0];
        }
        String nospacestr = str.replaceAll("\\s", ""); // take away spaces
        if (UnsignedInts.remainder(nospacestr.length(), 2) != 0) {
            throw new NumberFormatException("String should contain only pairs of [0-F] (should be even)!");
        }
        int expectedLength = nospacestr.length() / 2;
        String[] byteBlocks = nospacestr.split("(?<=\\G.{2})"); // split in parts of length 2
        if (expectedLength != byteBlocks.length) {
            System.out.println("Blocks: " + Arrays.toString(byteBlocks));
            throw new NumberFormatException("String should contain only pairs of [0-F]!");
        }
        byte[] bytes = new byte[byteBlocks.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = UnsignedBytes.parseUnsignedByte(byteBlocks[i], 16);
        }
        return bytes;
    }
}
