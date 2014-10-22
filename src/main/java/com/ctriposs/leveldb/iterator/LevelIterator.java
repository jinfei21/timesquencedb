package com.ctriposs.leveldb.iterator;

import java.util.Map.Entry;

import com.ctriposs.leveldb.ISeekIterator;
import com.ctriposs.leveldb.table.InternalKey;
import com.ctriposs.leveldb.table.Slice;

public class LevelIterator implements ISeekIterator<InternalKey, Slice> {

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Entry<InternalKey, Slice> next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
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
	public void prev() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	

}
