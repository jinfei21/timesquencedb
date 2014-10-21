package com.ctriposs.leveldb.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ctriposs.leveldb.ILogWriter;
import com.ctriposs.leveldb.table.Slice;
import com.google.common.io.Closeables;

public class PureFileLogWriter implements ILogWriter{
	private final File file;
	private final long fileNumber;
	private final FileChannel fileChannel;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private long offset;
	
	public PureFileLogWriter(File file, long fileNumber) throws IOException {
		this.file = file;
		this.fileNumber = fileNumber;
		this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
		this.offset = 0;
	}

	@Override
	public boolean isClosed() {
		return closed.get();
	}

	@Override
	public synchronized void close() throws IOException {
		closed.set(true);
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
	public synchronized void addRecord(Slice record, boolean force) throws IOException {
		
		
	}

}
