package com.ctriposs.tsdb.storage;

import java.io.Serializable;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.util.ByteUtil;

public class IndexHead implements Serializable{
	
	public static final int HEAD_SIZE = 4 + 16 + 16;
	
	public static final int COUNT_OFFSET = 0;
	public static final int MIN_CODE_OFFSET = 4;
	public static final int MIN_TIME_OFFSET = 12;
	public static final int MAX_CODE_OFFSET = 20;
	public static final int MAX_TIME_OFFSET = 28;
	private int count;
    private final InternalKey smallest;
    private final InternalKey largest;
    
    public IndexHead(int count,InternalKey smallest,InternalKey largest){
    	this.count = count;
    	this.smallest = smallest;
    	this.largest = largest;
    }
    
    public IndexHead(byte[] bytes){
    	this.count = ByteUtil.ToInt(bytes, COUNT_OFFSET);
    	this.smallest = new InternalKey(ByteUtil.ToLong(bytes, MIN_CODE_OFFSET), ByteUtil.ToLong(bytes, MIN_TIME_OFFSET));
    	this.largest = new InternalKey(ByteUtil.ToLong(bytes, MAX_CODE_OFFSET), ByteUtil.ToLong(bytes, MAX_TIME_OFFSET));
    }
    

	public byte[] toByte(){
		byte[] bytes = new byte[HEAD_SIZE];
		System.arraycopy(ByteUtil.toBytes(count), 0, bytes, COUNT_OFFSET, 4);
		System.arraycopy(ByteUtil.toBytes(smallest.getCode()), 0, bytes, MIN_CODE_OFFSET, 8);
		System.arraycopy(ByteUtil.toBytes(smallest.getTime()), 0, bytes, MIN_TIME_OFFSET, 8);
		System.arraycopy(ByteUtil.toBytes(largest.getCode()), 0, bytes, MIN_CODE_OFFSET, 8);
		System.arraycopy(ByteUtil.toBytes(largest.getTime()), 0, bytes, MIN_TIME_OFFSET, 8);	
		return bytes;
	}
}
