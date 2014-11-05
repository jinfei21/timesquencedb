package com.ctriposs.tsdb.table;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.tsdb.ILogReader;
import com.ctriposs.tsdb.common.IStorage;
import com.ctriposs.tsdb.common.MapFileStorage;

public class MapFileLogReader implements ILogReader{
	
	private IStorage storage;
	private AtomicInteger current;
	
	public MapFileLogReader(File file) throws IOException {
		this.current = new AtomicInteger(0);
		this.storage = new MapFileStorage(file);
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
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<String, Short> getNameMap() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
