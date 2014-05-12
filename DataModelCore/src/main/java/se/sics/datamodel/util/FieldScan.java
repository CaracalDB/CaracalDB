/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package se.sics.datamodel.util;

import se.sics.datamodel.QueryType;
import java.io.IOException;
import java.util.Objects;
import org.javatuples.Pair;
import se.sics.caracaldb.KeyRange;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class FieldScan implements QueryType {
    public final Object from;
    public final Object to;
    
    public FieldScan(Object from, Object to) {
        this.from = from;
        this.to = to;
    }
    
    @Override
    public KeyRange getIndexRange(Pair<ByteId, ByteId> typeId, ByteId indexId) throws IOException {
        return DMKeyFactory.getIndexRangeBetween(typeId.getValue0(), typeId.getValue1(), indexId, from, to);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 11 * hash + Objects.hashCode(this.from);
        hash = 11 * hash + Objects.hashCode(this.to);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final FieldScan other = (FieldScan) obj;
        if (!Objects.equals(this.from, other.from)) {
            return false;
        }
        if (!Objects.equals(this.to, other.to)) {
            return false;
        }
        return true;
    }
}