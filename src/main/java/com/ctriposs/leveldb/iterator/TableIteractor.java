package com.ctriposs.leveldb.iterator;

import java.util.Map.Entry;

import com.ctriposs.leveldb.ISeekIterator;
import com.ctriposs.leveldb.table.Slice;
import com.ctriposs.leveldb.table.Table;

public class TableIteractor implements ISeekIterator<Slice, Slice>{

	private final Table table;
	
	public TableIteractor(Table table){
		this.table = table;
	}
	
	@Override
	public boolean hasNext() {


		return false;
	}

	@Override
	public Entry<Slice, Slice> next() {


		return null;
	}

	@Override
	public void remove() {


		
	}

	@Override
	public void seek(Slice key) {


		
	}

	@Override
	public byte[] key() {


		return null;
	}

	@Override
	public byte[] value() {
		
		return null;
	}

	@Override
	public boolean valid() {
		
		return false;
	}

	@Override
	public void prev() {
		
		
	}

	@Override
	public void close() {
		
		
	}
}
