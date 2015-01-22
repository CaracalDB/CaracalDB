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
package se.sics.caracaldb.persistence;

import com.typesafe.config.Config;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class Database implements Persistence {
    
    public static enum Level {
        HOST,
        VNODE;
    }
    
    /**
     * For the DatabaseManager to be able to instantiate Database implementations
     * they require a constructor that takes nothing but a Config object.
     * If you need additional parameters, write them into the config file and read
     * them from the Config object in the constructor.
     * @param config 
     */
    public Database(Config config) {
        
    }
    
    /**
     * Closes the database in an orderly fashion.
     * 
     * Should be idempotent, as it might be called multiple times.
     */
    public abstract void close();
    
    /**
     * Reveals if the database should have once instance per HOST or per VNODE.
     * 
     * This is important, especially for disk based databases, 
     * where you don't want the disk to spin like crazy due to parallel queries
     * to different regions.
     * 
     * However, VNODE level has better parallelisation (no global blocking) 
     * and thus improves performance significantly across vnodes when used.
     * 
     * @return the level on which this database's instance(s) should be started
     */
    public static Level level() {
        return Level.HOST; // Use the safer value by default, override (hide) for performance
    }
}
