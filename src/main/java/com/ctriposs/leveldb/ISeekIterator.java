package com.ctriposs.leveldb;

public interface ISeekIterator<T> {
	
	void seek(T key);
	
	byte[] key();
	
	byte[] value();
	
	void next();

	void prev();

	void close(); 
}
