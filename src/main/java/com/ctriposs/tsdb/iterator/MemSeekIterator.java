package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.table.MemTable;

public class MemSeekIterator implements ISeekIterator<InternalKey, byte[]> {
	
	private MemTable memTable;
	private Iterator<Entry<InternalKey, byte[]>> curSeeIterator;
	private Entry<InternalKey, byte[]> curEntry;
	private InternalKey seekKey;
	private FileManager fileManager;
	
	public MemSeekIterator(FileManager fileManager,MemTable memTable){
		this.memTable = memTable;
		this.fileManager = fileManager;
		this.curSeeIterator = null;
		this.seekKey = null;
		this.curEntry = null;
	}

	@Override
	public boolean hasNext() {

		if(curSeeIterator != null){
			return curSeeIterator.hasNext();
		}
		return false;
	}
	
	@Override
	public boolean hasPrev() {
		
		return false;
	}

	@Override
	public Entry<InternalKey, byte[]> next() {
		Entry<InternalKey, byte[]> entry = curEntry;
		if(curSeeIterator != null){
			curEntry = curSeeIterator.next();
		}
		return entry;
	}


	@Override
	public void seek(String table, String column, long time) throws IOException {

		seekKey = new InternalKey(fileManager.getCode(table),fileManager.getCode(column), time);
		ConcurrentSkipListMap<InternalKey, byte[]> list = memTable.getConcurrentSkipList(time);
		if(list != null){
			
			curSeeIterator = list.tailMap(seekKey).entrySet().iterator();
			curEntry = curSeeIterator.next();
		}
		
	}

	@Override
	public String table() {
		if(curEntry != null){
			return fileManager.getName(curEntry.getKey().getTableCode());
		}
		return null;
	}

	@Override
	public String column() {

		if (curEntry != null) {
			return fileManager.getName(curEntry.getKey().getColumnCode());
		}
		return null;
	}

	@Override
	public InternalKey key() {

		if (curEntry != null) {
			return curEntry.getKey();
		}
		return null;
	}

	@Override
	public long time() {
		if (curEntry != null) {
			return curEntry.getKey().getTime();
		}
		return 0;
	}

	@Override
	public byte[] value() throws IOException {
		if (curEntry != null) {
			return curEntry.getValue();
		}

		return null;
	}

	@Override
	public boolean valid() {
		if (curEntry == null) {
			return false;
		} else {
			return true;
		}
	}



	@Override
	public Entry<InternalKey, byte[]> prev() {

		return null;
	}

	@Override
	public void close() throws IOException {

	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}
	
}
