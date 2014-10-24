package com.ctriposs.tsdb.table;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.hash.BloomFilter;

public class MemTable {
	private final long MAX_MEM_SIZE = 128 * 1024 * 1024L;
    private final ConcurrentSkipListMap<InternalKey, byte[]> table;
	protected BloomFilter<byte[]> bloomFilter;
    private final int maxMemTableSize;    
    private AtomicLong used = new AtomicLong(0);
    public MemTable(int maxMemTableSize,InternalKeyComparator internalKeyComparator){
    	this.table = new ConcurrentSkipListMap<InternalKey, byte[]>(internalKeyComparator);
    	this.maxMemTableSize = maxMemTableSize;
    }
    
    public boolean isEmpty(){
    	return table.isEmpty();
    }
    
    public long getUsed(){
    	return used.get();
    }
    
    public boolean add(InternalKey key, byte value[]){
    	boolean result = true;
    	
    	int length = value.length + key.getUserKey().length;
    	if(used.addAndGet(length)>maxMemTableSize){
    		result = false;
    	}else{
    		table.put(key, value);
    	}
    	return result;
    }
    
    public Set<Map.Entry<InternalKey, byte[]>> entrySet(){
    	return this.table.entrySet();
    }
    
}
