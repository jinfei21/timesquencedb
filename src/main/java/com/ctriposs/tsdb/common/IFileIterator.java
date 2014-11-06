package com.ctriposs.tsdb.common;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.ctriposs.tsdb.storage.CodeItem;

public interface IFileIterator<K, V> extends Iterator<Entry<K, V>> {

	void seek(int code,long time) throws IOException;

	void seekToFirst(int code) throws IOException;
	
	void seekToCurrent(int code) throws IOException;

	CodeItem nextCode() throws IOException;
	
	CodeItem prevCode() throws IOException;	
	
	CodeItem currentCode() throws IOException;	
	
	boolean hasNextCode() throws IOException;
	
	boolean hasPrevCode() throws IOException;	
	
	long timeItemCount();
	
	long priority();
	
	K key();

	long time();

	byte[] value() throws IOException;

	boolean valid();
	
    boolean hasPrev();

	Entry<K, V> prev();

	void close() throws IOException;
}
