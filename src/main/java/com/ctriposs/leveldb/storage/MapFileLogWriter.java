package com.ctriposs.leveldb.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ctriposs.leveldb.Constant;
import com.ctriposs.leveldb.ILogWriter;
import com.ctriposs.leveldb.table.Slice;
import com.ctriposs.leveldb.util.ByteBufferUtil;
import com.ctriposs.leveldb.util.ByteUtil;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

public class MapFileLogWriter implements ILogWriter {

	private final File file;
	private final long fileNumber;
	private final FileChannel fileChannel;
	private final MappedByteBuffer mappedByteBuffer;
	private final AtomicBoolean closed = new AtomicBoolean(false);

	public MapFileLogWriter(File file, long fileNumber) throws IOException {
		this.file = file;
		this.fileNumber = fileNumber;
		this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
		this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0,Constant.PAGE_SIZE);
	}

	@Override
	public boolean isClosed() {

		return closed.get();
	}

	@Override
	public synchronized void close() throws IOException {
		closed.set(true);
		ByteBufferUtil.unmap(mappedByteBuffer);
		if (fileChannel.isOpen()) {
			fileChannel.truncate(0);
		}
		Closeables.close(fileChannel, false);
	}

	@Override
	public void delete() throws IOException {
		close();
		file.delete();
	}

	@Override
	public File getFile() {
		return file;
	}

	@Override
	public long getFileNumber() {
		return fileNumber;
	}

	@Override
	public synchronized void addRecord(Slice key,Slice value, boolean force)
			throws IOException {
		Preconditions.checkState(!closed.get(), "Log has been closed");
        mappedByteBuffer.put(ByteUtil.toBytes(key.getData().length));
        mappedByteBuffer.put(ByteUtil.toBytes(value.getData().length));
        mappedByteBuffer.put(key.getData());
        mappedByteBuffer.put(value.getData());
		
		if (force) {
			mappedByteBuffer.force();
		}
	}
}
