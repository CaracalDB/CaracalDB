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
package se.sics.caracaldb.operations;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public enum ResponseCode {

    SUCCESS(0),
    BUSY(1),
    LOOKUP_TIMEOUT(2),
    READ_TIMEOUT(3),
    WRITE_TIMEOUT(4),
    CLIENT_TIMEOUT(5),
    RANGEQUERY_TIMEOUT(6),
    SUCCESS_INTERRUPTED(7),
    UNSUPPORTED_OP(8);
    
    public final byte id; // Could use ordinal() but I feel explicit assignment is safer, though slower

    private ResponseCode(int id) {
        this.id = (byte) id;
    }
    
    public static ResponseCode byId(byte id) {
        for (ResponseCode rc : ResponseCode.values()) {
            if (rc.id == id) {
                return rc;
            }
        }
        return null;
    }
    
}
