package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.ctriposs.tsdb.storage.IndexMeta;

public interface IFileIterator<K, V> extends Iterator<Entry<K, V>> {

	void seek(int code) throws IOException;

	void seekToFirst() throws IOException;

	IndexMeta nextMeta() throws IOException;
	
	IndexMeta prevMeta() throws IOException;	
	
	K key();

	long time();

	byte[] value() throws IOException;

	boolean valid();

	Entry<K, V> prev();

	void close() throws IOException;
}
