package com.ctriposs.tsdb.table;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.storage.DataMeta;

public class MemTable {
	public final static long MAX_MEM_SIZE = 256 * 1024 * 1024L;

	private final ConcurrentHashMap<Long, ConcurrentSkipListMap<InternalKey, byte[]>> table;
	private final int maxMemTableSize;
	private final AtomicLong used = new AtomicLong(0);
	private Lock lock = new ReentrantLock();
	private InternalKeyComparator internalKeyComparator;
	
	public MemTable(int maxMemTableSize,InternalKeyComparator internalKeyComparator) {
		this.table = new ConcurrentHashMap<Long, ConcurrentSkipListMap<InternalKey, byte[]>>();
		this.maxMemTableSize = maxMemTableSize;
		this.internalKeyComparator = internalKeyComparator;
	}

	public boolean isEmpty() {
		return table.isEmpty();
	}

	public long getUsed() {
		return used.get();
	}

	private long format(long time){
		return time/1000*1000;
	}

	public boolean add(InternalKey key, byte value[]) {
		boolean result = true;

		int length = value.length + DataMeta.META_SIZE;
		if (used.addAndGet(length) > maxMemTableSize) {
			result = false;
		} else {
			long ts = format(key.getTime());
			ConcurrentSkipListMap<InternalKey, byte[]> slot = table.get(ts);
					
			if(slot==null){
				try{
					lock.lock();
					slot = table.get(ts);
					if(slot==null){	
						slot = new ConcurrentSkipListMap<InternalKey, byte[]>(internalKeyComparator);
						table.put(ts, slot);
					}
				}finally{
					lock.unlock();
				}
			}
			slot.put(key, value);
		}
		return result;
	}
	
	public byte[] getValue(InternalKey key){
		long ts = format(key.getTime());
		ConcurrentSkipListMap<InternalKey, byte[]> slot = table.get(ts);
		if(slot != null){
			return slot.get(key);
		}else{
			return null;
		}
	}
	
	public ConcurrentHashMap<Long,ConcurrentSkipListMap<InternalKey, byte[]>> getTable(){
		return this.table;
	}

}
