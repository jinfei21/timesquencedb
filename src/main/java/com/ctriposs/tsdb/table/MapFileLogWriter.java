package com.ctriposs.tsdb.table;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.tsdb.ILogWriter;
import com.ctriposs.tsdb.common.IStorage;
import com.ctriposs.tsdb.common.MapFileStorage;
import com.ctriposs.tsdb.storage.FileName;
import com.ctriposs.tsdb.util.ByteUtil;

public class MapFileLogWriter implements ILogWriter {
	
	private IStorage storage;
	private AtomicInteger current;
	
	public MapFileLogWriter(String dir, long fileNumber, long capacity) throws IOException {
		this.current = new AtomicInteger(0);
		this.storage = new MapFileStorage(dir, System.currentTimeMillis(), FileName.logFileName(fileNumber), capacity);
	}
	
	public MapFileLogWriter(String dir, String fileName, long capacity) throws IOException {
		this.current = new AtomicInteger(0);
		this.storage = new MapFileStorage(dir, System.currentTimeMillis(),fileName, capacity);
	}

	@Override
	public void close() throws IOException {
		this.storage.close();
	}

	@Override
	public void add(int code, long time, byte[] value) throws IOException {
		int metaOffset = current.getAndAdd(20 + value.length);
		storage.put(metaOffset + 0, ByteUtil.toBytes(code));
		storage.put(metaOffset + 4, ByteUtil.toBytes(time));
		storage.put(metaOffset + 12, ByteUtil.toBytes(value.length));
		storage.put(metaOffset + 16, value);
	}

    public void add(String name, short code) throws IOException {
        byte[] nameBytes = ByteUtil.ToBytes(name);
        int offset = current.getAndAdd(2 + nameBytes.length);
        storage.put(offset, ByteUtil.toBytes(code));
        storage.put(offset + 2, nameBytes);
    }
    
	@Override
	public String getName() {
		return storage.getName();
	}
}
