package com.ctriposs.tsdb.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.ctriposs.tsdb.IStorage;

public class PureFileStorage implements IStorage {

	private FileChannel fileChannel;
	private RandomAccessFile raf;
	private String fullFileName;
	public PureFileStorage(String dir, long time,String suffix, long capacity) throws IOException {
		File dirFile = new File(dir);
		if (!dirFile.exists()) {
            dirFile.mkdirs();
        }

		fullFileName = dir + time+"-"+suffix;
		raf = new RandomAccessFile(fullFileName, "rw");
		raf.setLength(capacity);
		fileChannel = raf.getChannel();
	}

	public PureFileStorage(File file, long capacity) throws IOException {
		raf = new RandomAccessFile(file, "rw");
		fullFileName = file.getPath();
		raf.setLength(capacity);
		fileChannel = raf.getChannel();
	}
	
	@Override
	public void get(int position, byte[] dest) throws IOException {
		fileChannel.read(ByteBuffer.wrap(dest), position);
	}

	@Override
	public void put(int position, byte[] source) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(source);

        while (byteBuffer.hasRemaining()) {
            int len = fileChannel.write(byteBuffer, position);
            position += len;
        }
	}
	
	@Override
	public void put(int position, ByteBuffer source) throws IOException {

        while (source.hasRemaining()) {
            int len = fileChannel.write(source, position);
            position += len;
        }
	}

	@Override
	public void free() {
		// nothing to do here
		try {
			fileChannel.truncate(0);
		} catch (IOException e) {
		}
	}

	@Override
	public void close() throws IOException {
		if (this.fileChannel != null) {
			this.fileChannel.close();
		}
		if (this.raf != null) {
			this.raf.close();
		}
	}

	@Override
	public String getName() {
		return fullFileName;
	}


}
