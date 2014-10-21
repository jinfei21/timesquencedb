package com.ctriposs.leveldb.iterator;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import com.ctriposs.leveldb.ISeekIterator;
import com.ctriposs.leveldb.table.InternalKey;
import com.ctriposs.leveldb.table.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class MemTableIterator implements ISeekIterator<InternalKey, Slice> {

	private final ConcurrentSkipListMap<InternalKey, Slice> table;
	private PeekingIterator<Entry<InternalKey, Slice>> iterator;
	private Entry<InternalKey, Slice> entry;
	public MemTableIterator(ConcurrentSkipListMap<InternalKey, Slice> table){
		Preconditions.checkNotNull(table, "table is null!");
		this.table = table;
		this.iterator = Iterators.peekingIterator(table.entrySet().iterator());
		this.entry = null;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Entry<InternalKey, Slice> next() {
		entry = iterator.next();
		return entry;
	}


	@Override
	public void seek(InternalKey key) {
		iterator = Iterators.peekingIterator(table.tailMap(key).entrySet().iterator());
	}

	@Override
	public byte[] key() {
		if(entry !=null){
			return entry.getKey().getUserKey().getData();
		}else{
			return null;
		}
	}

	@Override
	public byte[] value() {
		if(entry != null){
			return entry.getValue().getData();
		}else{
			return null;
		}
		
	}

	@Override
	public boolean valid() {
		if(entry != null){
			return true;
		}else{
			return false;
		}
	}

	@Override
	public void prev() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void close() {
		throw new UnsupportedOperationException();
	}



}
