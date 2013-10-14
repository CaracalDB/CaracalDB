/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public abstract class Limit {
    public static LimitTracker noLimit() {
        return new LimitTracker() {

            @Override
            public boolean read(byte[] value) {
                return true;
            }

            @Override
            public boolean canRead() {
                return true;
            }
        };
    }
    
    public static LimitTracker toBytes(int number) {
        return new ByteSize(number);
    }
    
    public static LimitTracker toKiloBytes(int number) {
        return new ByteSize(number*1000);
    }
    
    public static LimitTracker toMegaBytes(int number) {
        return new ByteSize(number*1000*1000);
    }
    
    public static LimitTracker toItems(int number) {
        return new ItemCount(number);
    }
    
    public interface LimitTracker {
        /**
         * @param value
         * @return <canReanCurrent>
         */
        public boolean read(byte[] value);
        
        /**
         * @return <canReadFurther>
         */
        public boolean canRead(); 
    }
    
    private static class ItemCount implements LimitTracker {
        private int itemCount;
        
        public ItemCount(int itemCount) {
            this.itemCount = itemCount;
        }
        
        @Override
        public boolean read(byte[] value) {
            if(itemCount >= 1) {
                itemCount--;
                return true;
            }
            return false;
        }
        
        @Override
        public boolean canRead() {
            return itemCount >= 1;
        }
    }
    
    private static class ByteSize implements LimitTracker {
        private int byteSize;
        
        public ByteSize(int byteSize) {
            this.byteSize = byteSize;
        }
        
        @Override 
        public boolean read(byte[] value) {
            if(byteSize >= value.length) {
                byteSize = byteSize - value.length;
                return true;
            }
            return false;
        }
        
        @Override 
        public boolean canRead() {
            return byteSize > 0;
        }
    }
}

