package com.ctriposs.leveldb.util;

import com.ctriposs.leveldb.table.ValueType;

public class SequenceUtil {

	public static long packSequenceAndValueType(long sequence,ValueType valueType){
		
		return (sequence << 8) | valueType.getPersistentId();
	}
	
	public static ValueType unpackValueType(long num){
		return ValueType.getValueTypeByPersistentId((byte)num);
	}
	
	public static long unpackSequence(long num){
		return num >> 8;
	}
	
}
