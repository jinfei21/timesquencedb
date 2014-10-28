package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

public interface IInternalSeekIterator<K, V> extends Iterator<Entry<K, V>> {

	void seek(long code) throws IOException;

	void seekToFirst() throws IOException;

	String table();

	String column();

	K key();

	long time();

	byte[] value() throws IOException;

	boolean valid();

	Entry<K, V> prev();

	void close() throws IOException;
}
