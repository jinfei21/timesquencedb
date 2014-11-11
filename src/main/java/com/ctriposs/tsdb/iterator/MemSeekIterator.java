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
	
	private ConcurrentSkipListMap<InternalKey, byte[]> dataMap;
	private Iterator<Entry<InternalKey, byte[]>> curSeeIterator;
	private Entry<InternalKey, byte[]> curEntry;
	private InternalKey seekKey;
	private FileManager fileManager;
	private long fileNumber;
	public MemSeekIterator(FileManager fileManager,MemTable memTable,long fileNumber){
		this.dataMap = memTable.getAllConcurrentSkipList();
		this.fileManager = fileManager;
		this.fileNumber = fileNumber;
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
		if(curEntry != null){
			if(null == dataMap.lowerEntry(curEntry.getKey())){
				return false;
			}else{
				return true;
			}
		}
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
	public Entry<InternalKey, byte[]> prev() {
		Entry<InternalKey, byte[]> entry = curEntry;
		if(curSeeIterator != null){
			if(curEntry != null){
				curEntry = dataMap.lowerEntry(curEntry.getKey());
			}
		}
		return entry;

	}


	@Override
	public void seek(String table, String column, long time) throws IOException {

		seekKey = new InternalKey(fileManager.getCode(table),fileManager.getCode(column), time);		
		curSeeIterator = dataMap.tailMap(seekKey,true).entrySet().iterator();
		curEntry = curSeeIterator.next();
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
	public void close() throws IOException {
		dataMap.clear();
		dataMap = null;
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}

	@Override
	public long priority() {
		return 0L;
	}

}
