package com.ctriposs.leveldb.table;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ctriposs.leveldb.Constant;
import com.google.common.base.Preconditions;

public class Slice implements Comparable<Slice>{

	private final byte[] data;
	private final int offset;
	private final int length;
	private int hash;

	public Slice(int length){
		this.data = new byte[length];
		this.offset = 0;
		this.length = length;
	}
	
	public Slice(byte[] data){
		Preconditions.checkNotNull(data, "array is null!");
		this.data = data;
		this.offset = 0;
		this.length = data.length;
	}
	
	public Slice(byte[] data,int offset,int length){
		Preconditions.checkNotNull(data, "array is null");
		this.data = data;
		this.offset = offset;
		this.length = length;
	}
	
	public int length(){
		return length;
	}
	
	public byte[] getData(){
		return data;
	}
	
	public int getOffset(){
		return offset;
	}
	
	public byte getByte(int index){
		Preconditions.checkPositionIndexes(index, index + Constant.SIZE_OF_BYTE, length);
		index += offset;
		return data[index];
	}
	
	public short getUnsignedByte(int index){
		return (short)(getByte(index) & 0xFF);
	}
	
	public short getShort(int index){
		Preconditions.checkPositionIndexes(index, index + Constant.SIZE_OF_SHORT, length);
		index += offset;
		return (short)(data[index]&0xFF|
			  (data[index+1]&0xFF)<<8);
	}
	
	public int getInt(int index){
		Preconditions.checkPositionIndexes(index, index + Constant.SIZE_OF_INT, length);
		index += offset;
		return  (data[index]&0xFF|
				(data[index+1]&0xFF)<<8|
				(data[index+2]&0xFF)<<16|
				(data[index+3]&0xFF)<<24);
	}

	public long getLong(int index){
		Preconditions.checkPositionIndexes(index, index+Constant.SIZE_OF_LONG, length);
		index += offset;
		return ((long)data[index]&0xFF)|
				((long)data[index+1]&0xFF)<<8|
				((long)data[index+2]&0xFF)<<16|
				((long)data[index+3]&0xFF)<<24|
				((long)data[index+4]&0xFF)<<32|
				((long)data[index+5]&0xFF)<<40|
				((long)data[index+6]&0xFF)<<48|
				((long)data[index+7]&0xFF)<<56;
	}
	
	public void getBytes(int index,OutputStream out,int length)throws IOException{
		Preconditions.checkPositionIndexes(index, index + length, this.length);
		index += offset;
		out.write(data, index, length);
	}
	
	public void getBytes(int index,byte[] dest,int destIndex,int length){
		Preconditions.checkPositionIndexes(index, index + length, this.length);
		Preconditions.checkPositionIndexes(destIndex, destIndex + length, dest.length);
		index += offset;
		System.arraycopy(data, index, dest, destIndex, length);
	}
	
	public void setByte(int index,int value){
		Preconditions.checkPositionIndexes(index, index + Constant.SIZE_OF_BYTE, this.length);
		index += offset;
		data[index] = (byte)value;
	}
	
	public Slice copySlice(){
		return copySlice(0,length);
	}
	
	public Slice copySlice(int index,int length){
		Preconditions.checkPositionIndexes(index, index + length, this.length);
		index += offset;
		byte[] array = new byte[length];
		System.arraycopy(data, index, array, 0, length);
		return new Slice(array);
	}
	
	public ByteBuffer toByteBuffer(){
		return toByteBuffer(0, length);
	}
	
	public ByteBuffer toByteBuffer(int index,int length){
		Preconditions.checkPositionIndexes(index, index + length, this.length);
		index += offset;
		return ByteBuffer.wrap(data, index, length).order(ByteOrder.LITTLE_ENDIAN);
	}
	
	@Override
	public int compareTo(Slice o) {
		if(this == o){
			return 0;
		}
		
		if(data == o.data && length == o.length && offset == o.offset){
			return 0;
		}
		
		int min = Math.min(length, o.length);
		
		for(int i=0;i<min;i++){
			int tByte = 0xFF & data[offset+i];
			int oByte = 0xFF & o.data[o.offset+i];
			if(tByte != oByte){
				return tByte - oByte;
			}
		}
		
		return 0;
	}
	
	@Override
	public boolean equals(Object o){
		if(this == o){
			return true;
		}
		if(o==null||getClass() != o.getClass()){
			return false;
		}
		
		Slice slice = (Slice)o;
		if(length != slice.length){
			return false;
		}
		
		if(offset == slice.offset&&data==slice.data){
			return true;
		}
		
		for(int i=0;i<length;i++){
			if(data[offset + i] != slice.data[slice.offset + i]){
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode(){
		if(hash != 0){
			return hash;
		}
		
		for(int i=offset;i<offset+length;i++){
			hash = 31*hash + data[i];
		}
		
		if(hash == 0){
			hash = 1;
		}
		return hash;
	}
	
}
