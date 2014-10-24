package com.ctriposs.tsdb;

import java.io.Closeable;
import java.io.IOException;

public interface IDB extends ISeekIterable<InternalKey, byte[]>, Closeable {

	void put(String tableName,String colName,long time,byte[] value)throws IOException;
	
	byte[] get(String tableName,String colName,long time);
	
	void delete(long afterTime);
}
