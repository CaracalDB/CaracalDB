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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import se.sics.caracaldb.system.Stats;
import se.sics.kompics.address.Address;

/**
 *
 * @author lkroll
 */
public interface MaintenancePolicy {
    /**
     * Initialise the policy.
     * @param lut Reference to the LookupTable to work with. READ ONLY!
     */
    public void init(LookupTable lut);
    /**
     * Calculate and return the updates to improve the state of the system.
     * 
     * Return null if no update is to be performed.
     * 
     * @param joins Nodes that want to join
     * @param fails Nodes that failed
     * @param stats Current system statistics from all alive nodes
     * @return 
     */
    public LUTUpdate rebalance(ImmutableSet<Address> joins, ImmutableSet<Address> fails, ImmutableMap<Address, Stats.Report> stats);
}
