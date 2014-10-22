package com.ctriposs.leveldb.storage;

import java.io.DataOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.ctriposs.leveldb.table.Slice;

public class SliceOutput extends OutputStream implements DataOutput {

	private final Slice slice;
	private int size;

	public SliceOutput(Slice slice) {
		this.slice = slice;
	}

	public void reset() {
		size = 0;
	}

	public int size() {
		return size;
	}

	public boolean isWritable() {
		return writableBytes() > 0;
	}

	public int writableBytes() {
		return slice.length() - size;
	}
	
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        slice.setBytes(size, source, sourceIndex, length);
        size += length;
    }

    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
    }
	@Override
	public void writeByte(int value) {
		slice.setByte(size++, value);
	}

	public ByteBuffer toByteBuffer() {
		return slice.toByteBuffer(0, size);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + '(' + "size=" + size + ", "
				+ "capacity=" + slice.length() + ')';
	}

	@Override
	public void writeBoolean(boolean v) {
        throw new UnsupportedOperationException();

	}



	@Override
	public void writeChar(int v)  {
        throw new UnsupportedOperationException();

	}

	@Override
	public void writeShort(int value) {
		slice.setShort(size, value);
		size+=4;
	}
	
	@Override
	public void writeInt(int value)  {
		slice.setInt(size, value);
		size+=4;
	}

	@Override
	public void writeLong(long value)  {
		slice.setLong(size, value);
		size+=8;
	}

	@Override
	public void writeFloat(float v)  {
        throw new UnsupportedOperationException();

	}

	@Override
	public void writeDouble(double v)  {
        throw new UnsupportedOperationException();

	}

	@Override
	public void writeBytes(String s) {
        throw new UnsupportedOperationException();

	}

	@Override
	public void writeChars(String s)  {
        throw new UnsupportedOperationException();

	}

	@Override
	public void writeUTF(String s)  {
        throw new UnsupportedOperationException();

	}

	@Override
	public void write(int b) {
        throw new UnsupportedOperationException();

	}

	public void writeZero(int length) {
		if (length == 0) {
			return;
		}
		if (length < 0) {
			throw new IllegalArgumentException("length must be 0 or greater than 0.");
		}
		int nLong = length >>> 3;
		int nBytes = length & 7;
		for (int i = nLong; i > 0; i--) {
			writeLong(0);
		}
		if (nBytes == 4) {
			writeInt(0);
		} else if (nBytes < 4) {
			for (int i = nBytes; i > 0; i--) {
				writeByte((byte) 0);
			}
		} else {
			writeInt(0);
			for (int i = nBytes - 4; i > 0; i--) {
				writeByte((byte) 0);
			}
		}
	}

}
