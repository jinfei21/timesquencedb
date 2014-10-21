package com.ctriposs.leveldb;

import java.util.Map.Entry;

public interface ISeekIterator<K,V> extends Iterable<Entry<K, V>> {
	
	void seek(K key);
	
	byte[] key();
	
	byte[] value();
	
	boolean valid();
	
	void next();

	void prev();

	void close(); 
}
