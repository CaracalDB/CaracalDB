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

import com.google.common.collect.LinkedListMultimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class ServiceRegistry {
    private HashMap<String, Service> serviceByName = new HashMap<String, Service>();
    private LinkedListMultimap<String, Service> serviceByClass = LinkedListMultimap.create();

    public void provide(Service s) {
        serviceByName.put(s.name, s);
        serviceByClass.put(s.type.getCanonicalName(), s);
    }

    public void unregister(String serviceName) {
        Service s = serviceByName.get(serviceName);
        if (s != null) {
            serviceByName.remove(s.name);
            serviceByClass.remove(s.type.getCanonicalName(), s);
        }
    }
    
    public Service forName(String name) {
        return serviceByName.get(name);
    }
    public List<Service> forClass(String name) {
        return serviceByClass.get(name);
    }
    public List<Service> forClass(Class<? extends PortType> clazz) {
        return forClass(clazz.getCanonicalName());
    }
    public List<Service> forClass(Object obj) {
        return forClass(obj.getClass());
    }
    public Collection<Service> listServices() {
        return serviceByName.values();
    }
}
