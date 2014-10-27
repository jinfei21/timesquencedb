package com.ctriposs.tsdb;

import java.util.Comparator;

import com.ctriposs.tsdb.util.ByteUtil;


public class InternalKey implements Comparator<InternalKey>{

	private long code;
	private long time;
	
	public InternalKey(int tableCode, int columnCode, long time) {
		this.code = ByteUtil.ToLong(tableCode, columnCode);
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
	public int compare(InternalKey o1, InternalKey o2) {
		int diff = (int) (o1.getCode() - o2.getCode());
		if(diff == 0){
			diff = (int) (o1.getTime() - o2.getTime());
		}
		return diff;
	}
}
