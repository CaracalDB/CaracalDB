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

import com.google.common.base.Objects;
import java.util.TreeMap;
import java.util.UUID;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;

/**
 * Just a container for responses collected by a RangeQuery.SeqCollector
 * <p>
 * @author sario
 */
public class RangeResponse extends CaracalResponse {

    public final TreeMap<Key, byte[]> results;
    public final KeyRange initRange;
    public final KeyRange coveredRange;

    public RangeResponse(UUID id, KeyRange initRange, ResponseCode code, KeyRange coveredRange, TreeMap<Key, byte[]> results) {
        super(id, code);
        this.initRange = initRange;
        this.coveredRange = coveredRange;
        this.results = results;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", this.id)
                .add("code", this.code)
                .add("initRange", this.initRange)
                .add("coveredRange", this.coveredRange)
                .add("#results", results.size()).toString();
    }

}
