package com.ctriposs.tsdb;

public interface IDB {

	void put(String table,String column,long ts,byte[] value);
	byte[] get(String table,String column,long ts);
	void delete(long after);
	
}
