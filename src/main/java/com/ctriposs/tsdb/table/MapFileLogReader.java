package com.ctriposs.tsdb.table;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.tsdb.ILogReader;
import com.ctriposs.tsdb.common.IStorage;
import com.ctriposs.tsdb.common.MapFileStorage;

public class MapFileLogReader implements ILogReader{
	
	private IStorage storage;
	private AtomicInteger current;
	private InternalKeyComparator internalKeyComparator;
	private long fileNumber;
	private File file;
	public MapFileLogReader(File file,long fileNumber,InternalKeyComparator internalKeyComparator) throws IOException {
		this.current = new AtomicInteger(0);
		this.storage = new MapFileStorage(file);
		this.internalKeyComparator = internalKeyComparator;
		this.fileNumber = fileNumber;
		this.file = file;
	}

	@Override
	public void close() throws IOException {
		storage.close();
	}

	@Override
	public String getName() {
		return storage.getName();
	}
	
	@Override
	public MemTable getMemTable() throws IOException {
		MemTable memTable = new MemTable(file,fileNumber,internalKeyComparator);
		
		return memTable;
	}
	
	@Override
	public Map<String, Short> getNameMap() throws IOException {
		 Map<String, Short> map = new HashMap<String,Short>();
		 
		 
		return map;
	}

}
