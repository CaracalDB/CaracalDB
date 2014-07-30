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
package se.sics.caracaldb.utils;

import com.google.common.collect.Ordering;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import org.javatuples.Pair;

/**
 *
 * @author sario
 * @param <K> key type
 * @param <V> value type
 */
public class TopKMap<K extends Comparable, V> {

    private ArrayList<Pair<K, V>> list = new ArrayList<Pair<K, V>>();
    private final PairFirstComparator<K, V> comp;
    public final int k;

    public TopKMap(int k) {
        this.k = k;
        Comparator<K> mycomp = Ordering.natural().reverse();
        this.comp = new PairFirstComparator<K, V>(mycomp);
    }

    public TopKMap(int k, Comparator<K> comp) {
        this.k = k;
        this.comp = new PairFirstComparator<K, V>(Ordering.from(comp).reverse());
    }

    public int size() {
        return list.size();
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public boolean containsKey(K key) {
        for (Pair<K, V> p : list) {
            if (comp.comp.compare(p.getValue0(), key) == 0) {
                return true;
            }
        }
        return false;
    }

    public boolean containsValue(V value) {
        for (Pair<K, V> p : list) {
            if (p.getValue1().equals(value)) {
                return true;
            }
        }
        return false;
    }

    public V get(K key) {
        for (Pair<K, V> p : list) {
            if (comp.comp.compare(p.getValue0(), key) == 0) {
                return p.getValue1();
            }
        }
        return null;
    }

    public V put(K key, V value) {
        list.add(new Pair(key, value));
        if (list.size() < k) {
            return value;
        } else {
            Collections.sort(list, comp);
            Pair<K, V> p = list.remove(k-1);
            return p.getValue1();
        }
    }

    public Entry<K, V> ceilingEntry() {
        if (list.isEmpty()) {
            return null;
        }
        Pair<K, V> p = list.get(0);
        return new AbstractMap.SimpleImmutableEntry<K, V>(p.getValue0(), p.getValue1());
    }

    public Entry<K, V> floorEntry() {
        if (list.isEmpty()) {
            return null;
        }
        Pair<K, V> p = list.get(list.size() - 1);
        return new AbstractMap.SimpleImmutableEntry<K, V>(p.getValue0(), p.getValue1());
    }

    public void clear() {
        list.clear();
    }

    public List<Entry<K, V>> entryList() {
        ArrayList<Entry<K, V>> newlist = new ArrayList<Entry<K, V>>(list.size());
        for (Pair<K, V> p : list) {
            newlist.add(new AbstractMap.SimpleImmutableEntry<K, V>(p.getValue0(), p.getValue1()));
        }
        return newlist;
    }
    
    public List<V> values() {
        ArrayList<V> newlist = new ArrayList<>(list.size());
        for (Pair<K, V> p : list) {
            newlist.add(p.getValue1());
        }
        return newlist;
    }
    
    public List<K> keys() {
        ArrayList<K> newlist = new ArrayList<>(list.size());
        for (Pair<K, V> p : list) {
            newlist.add(p.getValue0());
        }
        return newlist;
    }

    public static class PairFirstComparator<KEY extends Comparable, VALUE> implements Comparator<Pair<KEY, VALUE>> {

        public final Comparator<KEY> comp;

        public PairFirstComparator(Comparator<KEY> comp) {
            this.comp = comp;
        }

        @Override
        public int compare(Pair<KEY, VALUE> o1, Pair<KEY, VALUE> o2) {
            return comp.compare(o1.getValue0(), o2.getValue0());
        }

    }
}
