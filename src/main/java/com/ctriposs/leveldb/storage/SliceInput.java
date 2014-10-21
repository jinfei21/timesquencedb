package com.ctriposs.leveldb.storage;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import com.ctriposs.leveldb.table.Slice;

public class SliceInput extends InputStream implements DataInput{
	
	private Slice slice;
	private int position;
	
	public SliceInput(Slice slice){
		this.slice = slice;
	}
	
	public int position(){
		return position;
	}
	
	public void setPosition(int position){
		if(position < 0||position > slice.length()){
			throw new IndexOutOfBoundsException();
		}
		this.position = position;
	}
	
	public boolean isReadable(){
		return available() > 0;
	}
	
	public int available(){
		return slice.length() - position;
	}


	@Override
	public int read() throws IOException {		
		return readByte();
	}
	
	@Override
	public void readFully(byte[] dest) throws IOException {
		readBytes(dest);
	}

	@Override
	public void readFully(byte[] dest, int offset, int length) throws IOException {
		readBytes(dest, offset, length);
	}
	
	public void readBytes(byte[] dest){
		readBytes(dest, 0, dest.length);
	}
	
	public void readBytes(byte[] dest,int destIndex,int length){
		slice.getBytes(position, dest, destIndex, length);
		position += length;
	}

	@Override
	public int skipBytes(int length) throws IOException {
		length = Math.min(length, available());
		position += length;
		return length;
	}

	@Override
	public boolean readBoolean() throws IOException {
		
		return readByte() != 0;
	}

	@Override
	public byte readByte() throws IOException {
		if(position == slice.length()){
			throw new IndexOutOfBoundsException();
		}
		return slice.getByte(position++);
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return (short)(readByte()&0xFF);
	}

	@Override
	public short readShort() throws IOException {
		short v = slice.getShort(position);
		position += 2;
		return v;
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return readShort() & 0xFF;
	}

	@Override
	public int readInt() throws IOException {
		int n = slice.getInt(position);				
		position += 4;		
		return n;
	}

	@Override
	public long readLong() throws IOException {
		long l = slice.getLong(position);
		position += 8;
		return l;
	}

	@Override
	public char readChar() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public float readFloat() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public double readDouble() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readLine() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readUTF() throws IOException {
		throw new UnsupportedOperationException();
	}
}
