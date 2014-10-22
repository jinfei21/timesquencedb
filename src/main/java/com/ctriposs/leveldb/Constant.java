package com.ctriposs.leveldb;

import com.ctriposs.leveldb.table.Slice;

public class Constant {

	//byte
    public static final byte SIZE_OF_BYTE = 1;
    public static final byte SIZE_OF_SHORT = 2;
    public static final byte SIZE_OF_INT = 4;
    public static final byte SIZE_OF_LONG = 8;
    public static final long MAX_SEQUENCE =  ((0x01 << 56) - 1);
    
    //file
    public static final int PAGE_SIZE  = 1024 * 1024;
    public static final int BUFFER_SIZE = 4096 * 4;
    
    //compaction
    public static final int MAX_LEVELS = 7;
    public static final int LEVEL0_COMPACTION_THRESHOLD = 4;
    public static final int LEVEL0_SLOWDOWN_WRITES_THRESHOLD = 4;
    public static final int LEVEL0_STOP_WRITES_THRESHOLD = 4;
    public static final int COMPACTION_FILE_SIZE = 2 * 1048576;
    public final static int LEVEL0_DEFAULT_CAPACITY = 256 * 1024 * 1024; // 128M
    
    public static final Slice EMPTY_SLICE = new Slice(0);
    
	public final static int ITEM_META_SIZE = 8;
}
