package com.ctriposs.leveldb;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

public interface IEngine extends Iterable<Entry<byte[], byte[]>>, Closeable{

	/**
	 * Puts the value with the specified key.
	 *
	 * @param key the key
	 * @param value the value
	 * @throws IOException
	 */
	void put(byte[] key, byte[] value) throws IOException;

	/**
	 * Gets the value with the specified key.
	 *
	 * @param key the key
	 * @return the value
	 * @throws IOException
	 */
	byte[] get(byte[] key) throws IOException;

	/**
	 * Delete the value with the specified key.
	 *
	 * @param key the key
	 * @return the value
	 * @throws IOException
	 */
	byte[] delete(byte[] key) throws IOException;
	
	/**
	 * Close the engine.
	 *
	 * @throws IOException
	 */
	void close() throws IOException;
	
	/**
	 * get the engine iterator.
	 *
	 * @throws IOException
	 */
	ISeekIterator<byte[], byte[]> iterator();
}
