package com.ctriposs.tsdb.table;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.tsdb.ILogWriter;
import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.storage.IndexMeta;
import com.ctriposs.tsdb.storage.FileName;
import com.ctriposs.tsdb.storage.MapFileStorage;
import com.ctriposs.tsdb.util.ByteUtil;

public class MapFileLogWriter implements ILogWriter {
	
	private IStorage storage;
	private AtomicInteger current;
	
	public MapFileLogWriter(String dir, long fileNumber, long capacity) throws IOException {
		this.current = new AtomicInteger(0);
		this.storage = new MapFileStorage(dir, System.currentTimeMillis(), FileName.logFileName(fileNumber), capacity);
	}

	@Override
	public void close() throws IOException {
		this.storage.close();
	}

	@Override
	public void add(long code, long time, byte[] value) throws IOException {
		int metaOffset = current.get();
		storage.put(metaOffset + IndexMeta.CODE_OFFSET, ByteUtil.toBytes(code));
		storage.put(metaOffset + IndexMeta.TIME_OFFSET, ByteUtil.toBytes(time));
		storage.put(metaOffset + IndexMeta.VALUE_SIZE_OFFSET, ByteUtil.toBytes(value.length));
		storage.put(metaOffset + IndexMeta.VALUE_OFFSET_OFFSET, value);
		current.addAndGet(20 + value.length);
	}

	@Override
	public String getName() {
		return storage.getName();
	}
}
