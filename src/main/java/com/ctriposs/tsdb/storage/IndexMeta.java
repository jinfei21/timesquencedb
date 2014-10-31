package com.ctriposs.tsdb.storage;

import java.io.Serializable;
import java.util.Comparator;

import com.ctriposs.tsdb.util.ByteUtil;

public class IndexMeta implements Serializable, Comparator<IndexMeta> {

	public static final int META_SIZE = (Integer.SIZE + Long.SIZE + Integer.SIZE + Integer.SIZE) / Byte.SIZE;

	public static final int CODE_OFFSET = 0;
	public static final int TIME_OFFSET = 4;
	public static final int VALUE_SIZE_OFFSET = 12;
	public static final int VALUE_OFFSET_OFFSET = 16;

	public static final long TTL_NEVER_EXPIRE = -1L;
	public static final long TTL_DELETE = 0L;

	private int code;
	private long time;
	private int valueSize;
	private int valueOffSet;

	public IndexMeta(int valueOffSet) {
		this.code = 0;
		this.time = 0;
		this.valueSize = 0;
		this.valueOffSet = valueOffSet;
	}

	public IndexMeta(byte[] bytes) {
		this(bytes, 0);
	}

	public IndexMeta(byte[] bytes, int offSet) {
		this.code = ByteUtil.ToInt(bytes, offSet + CODE_OFFSET);
		this.time = ByteUtil.ToLong(bytes, offSet + TIME_OFFSET);
		this.valueSize = ByteUtil.ToInt(bytes, offSet + VALUE_SIZE_OFFSET);
		this.valueOffSet = ByteUtil.ToInt(bytes, offSet + VALUE_OFFSET_OFFSET);
	}

	public int getValueSize() {
		return valueSize;
	}

	public int getValueOffSet() {
		return valueOffSet;
	}

	public int getCode() {
		return code;
	}

	public long getTime() {
		return time;
	}

	@Override
	public int compare(IndexMeta o1, IndexMeta o2) {
		int diff = o1.getCode() - o1.getCode();

		if (diff == 0) {
			diff = (int) (o1.getTime() - o2.getTime());
		}
		return diff;
	}
}
