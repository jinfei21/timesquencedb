package com.ctriposs.leveldb.table;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.leveldb.ISeekIterator;
import com.google.common.base.Preconditions;

public class MemTable {
    private final ConcurrentSkipListMap<InternalKey, Slice> table;
    private final AtomicLong used = new AtomicLong();
    
    
    public MemTable(InternalKeyComparator internalKeyComparator){
    	this.table = new ConcurrentSkipListMap<InternalKey, Slice>(internalKeyComparator);
    }
    
    
    public boolean isEmpty(){
    	return table.isEmpty();
    }
    
    public long getUsed(){
    	return used.get();
    }
    
    public void add(long sequence,ValueType valueType,Slice key,Slice value){
    	InternalKey internalKey = new InternalKey(key,sequence,valueType);
    	table.put(internalKey, value);
    	used.addAndGet(key.length() + value.length());
    }
    
    public Slice get(LookupKey lookupKey){
    	Preconditions.checkNotNull(lookupKey, "lookup key is null!");
    	InternalKey internalKey = lookupKey.getInternalKey();    	
        Entry<InternalKey, Slice> entry = table.ceilingEntry(internalKey);
        if (entry == null) {
            return null;
        }
        
        InternalKey entryKey = entry.getKey();
        if (entryKey.getUserKey().equals(internalKey.getUserKey())) {
            if (entryKey.getValueType() == ValueType.DELETE) {
                return null;
            } else {
                return entry.getValue();
            }
        }
        return null;
    }
    
    class MemTableIterator implements ISeekIterator<InternalKey, Slice>{

		@Override
		public Iterator<Entry<InternalKey, Slice>> iterator() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void seek(InternalKey key) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public byte[] key() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public byte[] value() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean valid() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void next() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void prev() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}
    	
    	
    }
    
}
