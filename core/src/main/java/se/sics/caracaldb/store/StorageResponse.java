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

import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class StorageResponse extends Response {
    
    private long id;
    public final Diff diff;
    
    public StorageResponse(StorageRequest req, Diff diff) {
        super(req);
        id = req.getId();
        this.diff = diff;
    }
    
    /**
     * Set optional id to match up requests
     * 
     * @param val 
     */
    public long getId() {
        return id;
    }
}
