package com.ctriposs.leveldb;

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
    
    
    
}
