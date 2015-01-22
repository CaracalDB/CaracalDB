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
package se.sics.caracaldb.store;

import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.utils.ByteArrayRef;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Put extends StorageRequest {

    public final Key key;
    public final byte[] value;
    public final int versionId;

    public Put(Key k, byte[] v, int versionId) {
        key = k;
        value = v;
        this.versionId = versionId;
    }

    @Override
    public StorageResponse execute(Persistence store) {
        ByteArrayRef oldValue = store.get(key.getArray());

        Diff diff = null;
        if (value == null) { // this is actually the delete branch
            store.delete(key.getArray(), versionId);
            if (oldValue == null) {
                diff = new Diff(0, 0);
            } else {
                diff = new Diff(-(oldValue.length + key.getKeySize()), -1);
            }
        } else {
            store.put(key.getArray(), value, versionId);
            if (oldValue == null) {
                diff = new Diff(value.length + key.getKeySize(), 1);
            } else {
                diff = new Diff(value.length - oldValue.length, 0);
            }
        }
        return new PutResp(this, diff);
    }

    @Override
    public String toString() {
        return "PutReq(" + key + ", " + value + ")";
    }
}
