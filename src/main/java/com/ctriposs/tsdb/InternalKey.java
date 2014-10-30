package com.ctriposs.tsdb;

import com.ctriposs.tsdb.storage.IndexMeta;
import com.ctriposs.tsdb.util.ByteUtil;


public class InternalKey implements Comparable<InternalKey> {

	private long code;
	private long time;
	
	public InternalKey(int tableCode, int columnCode, long time) {
		this.code = ByteUtil.ToLong(tableCode, columnCode);
		this.time = time;
	}
	
	public InternalKey(long code,long time) {
		this.code = code;
		this.time = time;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public int getTableCode() {
		byte[] bytes = ByteUtil.toBytes(code);				
		return ByteUtil.ToInt(bytes, 0);
	}

	public int getColumnCode() {
		byte[] bytes = ByteUtil.toBytes(code);		
		return ByteUtil.ToInt(bytes, 4);
	}

	public long getCode(){
		return this.code;
	}
	
	public byte[] toByte(int valuesize,int valueoffset){
		byte[] bytes = new byte[IndexMeta.META_SIZE];
		System.arraycopy(ByteUtil.toBytes(code), 0, bytes, IndexMeta.CODE_OFFSET, 8);
		System.arraycopy(ByteUtil.toBytes(time), 0, bytes, IndexMeta.TIME_OFFSET, 8);
		System.arraycopy(ByteUtil.toBytes(valuesize), 0, bytes, IndexMeta.VALUE_SIZE_OFFSET, 4);
		System.arraycopy(ByteUtil.toBytes(valueoffset), 0, bytes, IndexMeta.VALUE_OFFSET_OFFSET, 4);
		return bytes;
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof InternalKey){
			InternalKey other = (InternalKey) o;
			if(code == other.code && time == other.time){
				return true;
			}
		}
		return false;
	}



	@Override
	public int compareTo(InternalKey o) {
		int diff = (int) (code - o.getCode());
		if(diff == 0){
			diff = (int) (time - o.getTime());
		}
		return diff;
	}
}
