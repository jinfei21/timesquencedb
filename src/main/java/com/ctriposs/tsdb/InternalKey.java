package com.ctriposs.tsdb;

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
