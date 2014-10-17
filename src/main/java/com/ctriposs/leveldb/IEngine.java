package com.ctriposs.leveldb;

import java.io.IOException;

public interface IEngine<K> {

	/**
	 * Puts the value with the specified key.
	 *
	 * @param key the key
	 * @param value the value
	 * @throws IOException
	 */
	void put(K key, byte[] value) throws IOException;
	
	/**
	 * Puts the value with specified key and time to idle in milliseconds.
	 * 
	 * @param key the key
	 * @param value the value
	 * @param ttl the time to idle value in milliseconds
	 * @throws IOException
	 */
	void put(K key, byte[] value, long ttl)  throws IOException;

	/**
	 * Gets the value with the specified key.
	 *
	 * @param key the key
	 * @return the value
	 * @throws IOException
	 */
	byte[] get(K key) throws IOException;

	/**
	 * Delete the value with the specified key.
	 *
	 * @param key the key
	 * @return the value
	 * @throws IOException
	 */
	byte[] delete(K key) throws IOException;
	
	/**
	 * Get iterator for seek.
	 *
	 * @return the iterator
	 * @throws IOException
	 */
	ISeekIterator iterator() throws IOException;
	
	/**
	 * Close the engine.
	 *
	 * @throws IOException
	 */
	void close() throws IOException;
	
}
