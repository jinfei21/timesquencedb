package com.ctriposs.tsdb.storage;

import com.ctriposs.tsdb.common.IStorage;
import com.ctriposs.tsdb.common.MapFileStorage;
import com.ctriposs.tsdb.util.ByteUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class NameFileWriter {

    private static final long DEFAULT_NAME_FILE_CAPACITY = 16 * 1024 * 1024L;
    private IStorage storage;
    private AtomicInteger current;

    public NameFileWriter(String dir) throws IOException {
        this.current = new AtomicInteger(0);
        this.storage = new MapFileStorage(dir, System.currentTimeMillis(), FileName.nameFileName(1), DEFAULT_NAME_FILE_CAPACITY);
    }

    public void add(String name, short code) throws IOException {
        byte[] nameBytes = ByteUtil.ToBytes(name);
        int offset = current.getAndAdd(2 + nameBytes.length);
        storage.put(offset, ByteUtil.toBytes(code));
        storage.put(offset + 2, nameBytes);
    }

    public void close() throws IOException {
        this.storage.close();
    }
}
