package com.ctriposs.tsdb;

import java.util.Iterator;
import java.util.Map.Entry;

public interface ISeekIterator<K,V> extends Iterator<Entry<K, V>> {
	
	void seek(String table,String column,long time);
	
	String table();
	
	String column();
	
	long time();
	
	byte[] value();
	
	boolean valid();
	
	void prev();

	void close(); 
}
