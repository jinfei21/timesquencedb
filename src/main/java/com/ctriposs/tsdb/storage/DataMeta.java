package com.ctriposs.tsdb.storage;

import java.io.Serializable;

public class DataMeta implements Serializable {


	public static final int META_SIZE = (Long.SIZE + Long.SIZE  + Integer.SIZE + Integer.SIZE) / Byte.SIZE;	
	
	public static final int CODE_OFFSET = 0;
	public static final int TIME_OFFSET = 8;
	public static final int VALUE_SIZE_OFFSET = 16;
	public static final int VALUE_OFFSET_OFFSET = 20;

	public static final long TTL_NEVER_EXPIRE = -1L;
	public static final long TTL_DELETE = 0L;

	private long code;
	private long time;
	private int valueSize;
	private int offSet;
	
	public DataMeta(int offSet) {
		this.code = 0;
		this.time = 0;
		this.valueSize = 0;
		this.offSet = offSet;
	}


	public int getValueSize() {
		return valueSize;
	}

	public void setValueSize(int valueSize) {
		this.valueSize = valueSize;
	}

	public int getOffSet() {
		return offSet;
	}

	public void setOffSet(int offSet) {
		this.offSet = offSet;
	}

}
