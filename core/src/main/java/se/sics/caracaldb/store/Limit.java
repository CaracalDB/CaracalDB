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

import java.io.Serializable;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public abstract class Limit {

    public static LimitTracker noLimit() {
        return new NoLimit();
    }

    public static LimitTracker toBytes(int number) {
        return new ByteSize(number);
    }

    public static LimitTracker toKiloBytes(int number) {
        return new ByteSize(number * 1000);
    }

    public static LimitTracker toMegaBytes(int number) {
        return new ByteSize(number * 1000 * 1000);
    }

    public static LimitTracker toItems(int number) {
        return new ItemCount(number);
    }

    /**
     * LimitTrackers are statefull
     */
    public interface LimitTracker extends Serializable {

        /**
         * @param value
         * @return <canReanCurrent>
         */
        public boolean read(byte[] value);

        /**
         * @return <canReadFurther>
         */
        public boolean canRead();

        public LimitTracker doClone();
    }

    private static class NoLimit implements LimitTracker, Serializable {

        @Override
        public boolean read(byte[] value) {
            return true;
        }

        @Override
        public boolean canRead() {
            return true;
        }

        @Override
        public NoLimit doClone() {
            return new NoLimit();
        }
    }

    private static class ItemCount implements LimitTracker, Serializable {

        private int itemCount;

        public ItemCount(int itemCount) {
            this.itemCount = itemCount;
        }

        @Override
        public boolean read(byte[] value) {
            if (itemCount >= 1) {
                itemCount--;
                return true;
            }
            return false;
        }

        @Override
        public boolean canRead() {
            return itemCount >= 1;
        }

        @Override
        public ItemCount doClone() {
            return new ItemCount(itemCount);
        }
    }

    private static class ByteSize implements LimitTracker, Serializable {

        private int byteSize;

        public ByteSize(int byteSize) {
            this.byteSize = byteSize;
        }

        @Override
        public boolean read(byte[] value) {
            if (byteSize >= value.length) {
                byteSize = byteSize - value.length;
                return true;
            }
            return false;
        }

        @Override
        public boolean canRead() {
            return byteSize > 0;
        }

        @Override
        public ByteSize doClone() {
            return new ByteSize(byteSize);
        }
    }
}
