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
package se.sics.caracaldb.truetime;

/**
 *
 * @author sario
 */
public class TrueTimeInterval {

    public final long earliest;
    public final long latest;

    TrueTimeInterval(long earliest, long latest) {
        this.earliest = earliest;
        this.latest = latest;
    }

    /**
     * Compares TrueTimeIntervals.
     * <p>
     * Not quite as compareTo: If return 0 then the intervals overlap and
     * neither is truly before of after the other. If the return is != 0 then
     * the return is difference between the interval boundaries.
     * <p>
     * @param ival
     * @return
     */
    public long compare(TrueTimeInterval ival) {
        if (ival.earliest > latest) {
            return ival.earliest - latest;
        }
        if (earliest > ival.latest) {
            return earliest - ival.latest;
        }
        return 0;
    }
}
