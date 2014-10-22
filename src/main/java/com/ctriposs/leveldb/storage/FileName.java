package com.ctriposs.leveldb.storage;

import com.google.common.base.Preconditions;

public class FileName {

	public static String logFileName(long number){
		return makeFileName(number, "log");
	}
	
	public static String tableFileName(long number){
		return makeFileName(number,"sst");
	}
	
	public static String indexFileName(long number){
		return makeFileName(number,"idx");
	}
	
	public static String dataFileName(long number){
		return makeFileName(number,"dat");
	}
	
	private static String makeFileName(long number,String suffix){
		Preconditions.checkArgument(number >=0 , "number is negative!");
		Preconditions.checkNotNull(suffix, "suffix is null!");
		return String.format("%06d.%s", number,suffix);
	}
}
