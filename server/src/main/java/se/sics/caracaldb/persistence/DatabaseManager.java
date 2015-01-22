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
import com.typesafe.config.ConfigValue;
import java.lang.reflect.Constructor;
import java.util.Map.Entry;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author lkroll
 */
public class DatabaseManager {

    private static final String PATH = "caracal.database.types";
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class);

    private final Config conf;
    //private final Map<TypeKey, Database> instances = new HashMap<TypeKey, Database>();

    /**
     * Preload all the defined store classes.
     *
     * Some stores (especially JNI based ones) need to unpack stuff and it's
     * better to do this early in the execution to avoid races later.
     *
     * @param conf
     */
    public static void preloadStores(Config conf) {
        LOG.info("Preloading stores...");
        for (Entry<String, ConfigValue> e : conf.getConfig(PATH).entrySet()) {
            ConfigValue cv = e.getValue();
            String className = (String) cv.unwrapped();
            try {
                Class<?> dbClass = ClassLoader.getSystemClassLoader().loadClass(className);
                if (!Database.class.isAssignableFrom(dbClass)) {
                    LOG.error("Class {} does not extend se.sics.caracaldb.persistence.Database!", className);
                    throw new ClassNotFoundException(className);
                }
                if (e.getKey().equals("leveldb")) { // LevelDB local deploy hack -.-
                    Database ldb = getInstance((Class<? extends Database>) dbClass, conf);
                    ldb.close();
                }
            } catch (ClassNotFoundException ex) {
                LOG.error("Could not load class " + className + "! Aborting!");
                System.exit(1); // it's better to abort early at this point
            } catch (InstantiationException ex) {
                LOG.error("Could not instantiate class " + className + "! Aborting!");
                System.exit(1); // it's better to abort early at this point    
            }
        }

    }

    public DatabaseManager(Config conf) {
        this.conf = conf;
    }

    public Class<? extends Database> getType(String identifier) throws ClassNotFoundException {
        String className = conf.getString(PATH + "." + identifier);
        Class<?> dbClass = ClassLoader.getSystemClassLoader().loadClass(className);
        if (!Database.class.isAssignableFrom(dbClass)) {
            LOG.error("Class {} does not extend se.sics.caracaldb.persistence.Database!", className);
            throw new ClassNotFoundException(className);
        }
        return (Class<? extends Database>) dbClass;
    }

    public Database getInstance(Class<? extends Database> type) throws InstantiationException {
        return DatabaseManager.getInstance(type, conf);
    }

    private static Database getInstance(Class<? extends Database> type, Config conf) throws InstantiationException {
        try {
//            TypeKey tk = new TypeKey(type.getName(), id);
//            Database db = instances.get(tk);
//            if (db != null) {
//                return db;
//            }
            Constructor cons = type.getConstructor(Config.class);
            Database db = (Database) cons.newInstance(conf);
            //instances.put(tk, db);
            return db;
        } catch (Exception ex) {
            LOG.error("Could not instantiate {}! Error was: \n {}", new Object[]{type, ex});
            ex.printStackTrace();
            throw new InstantiationException(type.getName());
        }
    }

//    public synchronized boolean dropInstance(Key id, Database instance) {
//        TypeKey tk = new TypeKey(instance.getClass().getName(), id);
//        Database db = instances.get(tk);
//        if (db != null) { 
//            // if a vnode asks for a host instance with his key, 
//            //this will fail, so a vnode can't accidentally close a host level db
//            instances.remove(tk);
//            db.close();
//            return true;
//        }
//        return false;
//    }
//    public static class TypeKey implements Comparable<TypeKey> {
//        
//        private final String classtype;
//        private final Key key;
//        
//        public TypeKey(String classtype, Key key) {
//            this.classtype = classtype;
//            this.key = key;
//        }
//        
//        @Override 
//        public boolean equals(Object o) {
//            if (o instanceof TypeKey) {
//                TypeKey that = (TypeKey) o;
//                return this.compareTo(that) == 0;
//            }
//            return false;
//        }
//        
//        @Override
//        public int hashCode() {
//            return Objects.hash(classtype, key);
//        }
//
//        @Override
//        public int compareTo(TypeKey that) {
//            return ComparisonChain.start()
//                    .compare(this.classtype, that.classtype)
//                    .compare(this.key, that.key).result();
//        }
//        
//    }
}
