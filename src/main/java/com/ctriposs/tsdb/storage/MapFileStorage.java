package com.ctriposs.tsdb.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.util.FileUtil;

public class MapFileStorage implements IStorage {

	private RandomAccessFile raf;
	private FileChannel fileChannel;
	private ThreadLocalByteBuffer threadLocalBuffer;
	private MappedByteBuffer mappedByteBuffer;
	private String fullFileName;
	public MapFileStorage(String dir, long time, long index,long capacity) throws IOException {
		File backFile = new File(dir);
		if (!backFile.exists()) {
			backFile.mkdirs();
		}
		fullFileName = dir + time+"-"+index + DATA_FILE_SUFFIX;
		raf = new RandomAccessFile(fullFileName, "rw");
		fileChannel = raf.getChannel();
		mappedByteBuffer = fileChannel.map(FileChannel.MapMode.PRIVATE, 0, capacity);
		threadLocalBuffer = new ThreadLocalByteBuffer(mappedByteBuffer);
	}
	
	public MapFileStorage(File file, long capacity) throws IOException {
		raf = new RandomAccessFile(file, "rw");
		fullFileName = file.getPath();
		mappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.PRIVATE, 0, capacity);
		threadLocalBuffer = new ThreadLocalByteBuffer(mappedByteBuffer);
	}

	private ByteBuffer getLocal(int position) {
		ByteBuffer buffer = threadLocalBuffer.get();
		buffer.position(position);
		return buffer;
	}

	@Override
	public void close() throws IOException {
		if (this.fileChannel != null) {
			this.fileChannel.close();
		}
		if (raf != null) {
			raf.close();
		}
		//implies system GC
		try {
			FileUtil.unmap(mappedByteBuffer);
		} catch (Throwable e) {
			throw new IOException(e);
		}
		threadLocalBuffer.set(null);
		threadLocalBuffer = null;

	}

	@Override
	public void get(int position, byte[] dest) throws IOException {
		ByteBuffer buffer = this.getLocal(position);
		buffer.get(dest);
	}

	@Override
	public void put(int position, byte[] source) throws IOException {
		ByteBuffer buffer = this.getLocal(position);
		buffer.put(source);
	}
	

	@Override
	public void put(int position, ByteBuffer source) throws IOException {
		ByteBuffer buffer = this.getLocal(position);
		buffer.put(source);
	}

	@Override
	public void free() {
		MappedByteBuffer buffer = (MappedByteBuffer) threadLocalBuffer.getSourceBuffer();
		buffer.clear();
		try {
			fileChannel.truncate(0);
		} catch (IOException e) {
		}
	}

	private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
		private ByteBuffer _src;

		public ThreadLocalByteBuffer(ByteBuffer src) {
			_src = src;
		}

		public ByteBuffer getSourceBuffer() {
			return _src;
		}

		@Override
		protected synchronized ByteBuffer initialValue() {
            return _src.duplicate();
		}
	}

	@Override
	public String getName() {
		return fullFileName;
	}


}
