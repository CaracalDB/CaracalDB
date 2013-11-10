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
package se.sics.caracaldb.system;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import se.sics.caracaldb.persistence.Database;
import se.sics.caracaldb.persistence.memory.InMemoryDB;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkr@lars-kroll.com>
 */
public class Configuration {

    public static enum SystemPhase {

        INIT,
        BOOTSTRAP_CLIENT,
        BOOTSTRAP_SERVER,
        BOOTSTRAPPED;
    }

    public static enum NodePhase {

        INIT,
        JOIN,
        SYNCED;
    }
    /*
     * Typesafe Config
     */
    private Config config;
    /*
     * CUSTOM
     */
    private boolean boot = true; // for testing
    private InetAddress ip;
//    private int port;
    private Multimap<SystemPhase, ComponentHook> hostHooks = HashMultimap.create();
    private Multimap<NodePhase, VirtualComponentHook> virtualHooks = HashMultimap.create();
    private Address bootstrapServer;
//    private int bootThreshold = 3;
//    private int bootVNodes = 1;
//    private long keepAlivePeriod = 1000;
//    private int messageBufferSizeMax = 16 * 1024;
//    private int messageBufferSize = messageBufferSizeMax/8;
//    private int dataMessageSize = messageBufferSize;
    /*
     * NO COPY
     */
    // Store
    private Database db;

    /*
     * Prevent instantiation outside Factory
     */
    private Configuration() {
    }
    
    /*
     * Custom Methods
     */
    public InetAddress getIp() {
        return ip;
    }

    public Iterable<ComponentHook> getHostHooks() {
        return hostHooks.values();
    }

    public Iterable<ComponentHook> getHostHooks(SystemPhase phase) {
        return hostHooks.get(phase);
    }

    public Iterable<VirtualComponentHook> getVirtualHooks() {
        return virtualHooks.values();
    }

    public Iterable<VirtualComponentHook> getVirtualHooks(NodePhase phase) {
        return virtualHooks.get(phase);
    }

    public Address getBootstrapServer() {
        return this.bootstrapServer;
    }

    public boolean isBoot() {
        return boot;
    }
    
    public int getPort() {
        return config.getInt("server.address.port");
    }

    /**
     * @return the db
     */
    public Database getDB() {
        return db;
    }

    /*
     * Conf Proxy Methods
     */
    public boolean hasPath(String path) {
        return config.hasPath(path);
    }

    public boolean isEmpty() {
        return config.isEmpty();
    }

    public Set<Entry<String, ConfigValue>> entrySet() {
        return config.entrySet();
    }

    public boolean getBoolean(String path) {
        return config.getBoolean(path);
    }

    public Number getNumber(String path) {
        return config.getNumber(path);
    }

    public int getInt(String path) {
        return config.getInt(path);
    }

    public long getLong(String path) {
        return config.getLong(path);
    }

    public double getDouble(String path) {
        return config.getDouble(path);
    }

    public String getString(String path) {
        return config.getString(path);
    }

    public ConfigObject getObject(String path) {
        return config.getObject(path);
    }

    public Object getAnyRef(String path) {
        return config.getAnyRef(path);
    }

    public Long getBytes(String path) {
        return config.getBytes(path);
    }

    public Long getMilliseconds(String path) {
        return config.getMilliseconds(path);
    }

    public Long getNanoseconds(String path) {
        return config.getNanoseconds(path);
    }

    public ConfigList getList(String path) {
        return config.getList(path);
    }

    public List<Boolean> getBooleanList(String path) {
        return config.getBooleanList(path);
    }

    public List<Number> getNumberList(String path) {
        return config.getNumberList(path);
    }

    public List<Integer> getIntList(String path) {
        return config.getIntList(path);
    }

    public List<Long> getLongList(String path) {
        return config.getLongList(path);
    }

    public List<Double> getDoubleList(String path) {
        return config.getDoubleList(path);
    }

    public List<String> getStringList(String path) {
        return config.getStringList(path);
    }

    public List<? extends ConfigObject> getObjectList(String path) {
        return config.getObjectList(path);
    }

    public List<? extends Object> getAnyRefList(String path) {
        return config.getAnyRefList(path);
    }

    public List<Long> getBytesList(String path) {
        return config.getBytesList(path);
    }

    public List<Long> getMillisecondsList(String path) {
        return config.getMillisecondsList(path);
    }

    public List<Long> getNanosecondsList(String path) {
        return config.getNanosecondsList(path);
    }

    private Configuration copy() {
        Configuration that = new Configuration();
        that.config = this.config; // Config is immutable
        that.boot = this.boot;
        that.bootstrapServer = this.bootstrapServer;
        that.hostHooks = HashMultimap.create(this.hostHooks);
        that.virtualHooks = HashMultimap.create(this.virtualHooks);
        that.ip = this.ip;
        // copying the store is not the smartest thing to do
        //that.db = this.db;

        return that;
    }

    public static abstract class Factory {

        public static Builder load() {
            return Configuration.Factory.load(ConfigFactory.load());
        }

        public static Builder load(Config config) {
            Configuration c = new Configuration();

            // Resolve IPs
            String ipStr = config.getString("bootstrap.address.hostname");
            String localHost = config.getString("server.address.hostname");
            int bootPort = config.getInt("bootstrap.address.port");
            InetAddress localIp = null;
            InetAddress bootIp = null;
            try {
                bootIp = InetAddress.getByName(ipStr);
                localIp = InetAddress.getByName(localHost);
            } catch (UnknownHostException ex) {
                throw new RuntimeException(ex.getMessage());
            }
            Address bootstrapServer = new Address(bootIp, bootPort, null);


            c.config = config;

            c.bootstrapServer = bootstrapServer;
            c.ip = localIp;

            c.db = new InMemoryDB(); // default

            return new Builder(c);
        }

        public static Builder modify(Configuration conf) {
            Builder b = new Builder(conf.copy());
            b.setDB(conf.db);
            return b;
        }

        public static Configuration copy(Configuration conf) {
            return conf.copy();
        }
    }

    public static class Builder {

        private Configuration conf;

        private Builder(Configuration conf) {
            this.conf = conf;
        }

        public Configuration finalise() {
            Configuration configuration = conf;
            conf = null; // to prohibit writing into it later
            return configuration;
        }

        public Builder setBootstrapServer(Address addr) {
            conf.bootstrapServer = addr;
            return this;
        }

        public Builder setBoot(boolean val) {
            conf.boot = val;
            return this;
        }

        public Builder addHostHook(SystemPhase phase, ComponentHook hook) {
            conf.hostHooks.put(phase, hook);
            return this;
        }

        public Builder addVirtualHook(NodePhase phase, VirtualComponentHook hook) {
            conf.virtualHooks.put(phase, hook);
            return this;
        }

        public Builder setIp(InetAddress ip) {
            conf.ip = ip;
            return this;
        }

        public Builder setValue(String path, ConfigValue value) {
            conf.config = conf.config.withValue(path, value);
            return this;
        }
        
        public Builder setValue(String path, Object value) {
            return setValue(path, ConfigValueFactory.fromAnyRef(value, "Builder modified"));
        }

        public Builder removeValue(String path) {
            conf.config = conf.config.withoutPath(path);
            return this;
        }
        
        public Builder setPort(int port) {
            return setValue("server.address.port", port);
        }

        /**
         * @param db the db to set
         */
        public Builder setDB(Database db) {
            conf.db = db;
            return this;
        }
    }
}
