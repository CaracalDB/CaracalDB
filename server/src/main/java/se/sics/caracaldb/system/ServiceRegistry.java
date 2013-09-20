/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
