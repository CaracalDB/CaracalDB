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

import java.io.IOException;
import java.util.UUID;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.kompics.Request;

/**
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public abstract class StorageRequest extends Request {

    private UUID id;
    private int versionId;

    /**
     * Custom interface for local storage queries.
     *
     * Implement the execute function, to do whatever operation you need to be
     * done on the persistent storage medium. You are guaranteed exclusive
     * access for the duration of the operation.
     *
     * ATTENTION: Do NOT pass the reference to the store to some other parent
     * object and access it later directly!!! For performance reasons the
     * reference is not temporary and not synchronised, i.e. you WILL be doing
     * parallel access to the database if you try this.
     *
     * @param store a reference to the backing store
     * @return either a response or null if no response is required
     */
    public abstract StorageResponse execute(Persistence store) throws IOException;

    /**
     * Set optional id to match up requests
     *
     * @param val
     */
    public void setId(UUID val) {
        this.id = val;
    }

    /**
     * Get optional id to match up requests
     *
     * @return id
     */
    public UUID getId() {
        return id;
    }
}
