package com.ctriposs.tsdb.table;

import java.util.Comparator;

import com.ctriposs.tsdb.util.ByteUtil;

public class InternalKey implements Comparator<InternalKey> {

	private byte[] key;

	public InternalKey(byte[] key){
		this.key = key;
	}

	@Override
	public int compare(InternalKey o1, InternalKey o2) {
		return ByteUtil.compare(o1.getUserKey(), o2.getUserKey());
	}
	
	public byte[] getUserKey(){
		return key;
	}
}
