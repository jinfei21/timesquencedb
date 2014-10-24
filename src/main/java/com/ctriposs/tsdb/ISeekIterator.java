package com.ctriposs.tsdb;

import java.util.Iterator;
import java.util.Map.Entry;

public interface ISeekIterator<K,V> extends Iterator<Entry<K, V>> {
	
	void seek(K key);
	
	void seekToFirst();
	
	byte[] key();
	
	byte[] value();
	
	boolean valid();
	
	void prev();

	void close(); 
}
