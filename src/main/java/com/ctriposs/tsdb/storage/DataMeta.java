package com.ctriposs.tsdb.storage;

import java.io.Serializable;
import java.util.Comparator;

import com.ctriposs.tsdb.util.ByteUtil;

public class DataMeta implements Serializable, Comparator<DataMeta> {


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
	
	public DataMeta(byte[] bytes){
		this.code = ByteUtil.ToLong(bytes,0);
		this.time = ByteUtil.ToLong(bytes, TIME_OFFSET);
		this.valueSize = ByteUtil.ToInt(bytes, VALUE_SIZE_OFFSET);
		this.offSet = ByteUtil.ToInt(bytes, VALUE_OFFSET_OFFSET);
	}

	public int getValueSize() {
		return valueSize;
	}

	public int getOffSet() {
		return offSet;
	}

	public long getCode() {
		return code;
	}

	public long getTime() {
		return time;
	}

    @Override
    public int compare(DataMeta o1, DataMeta o2) {
        int diff = (int) (o1.getCode() - o1.getCode());

        if (diff == 0) {
            diff = (int) (o1.getTime() - o2.getTime());
        }
        return diff;
    }
}
