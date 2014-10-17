package com.ctriposs.leveldb;

import java.io.File;
import java.io.IOException;

import com.ctriposs.leveldb.util.FileUtil;

public class DBEngine<K> implements IEngine<K> {
	
	
	
    
	
	public DBEngine(String dir, EngineConfig config) throws IOException {
    	
        String cacheDir = dir;
		if (!cacheDir.endsWith(File.separator)) {
			cacheDir += File.separator;
		}
		// validate directory
		if (!FileUtil.isFilenameValid(cacheDir)) {
			throw new IllegalArgumentException("Invalid storage data directory : " + cacheDir);
		}
     	
		
		
    	
    }

	@Override
	public void put(K key, byte[] value) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void put(K key, byte[] value, long ttl) throws IOException {
		
		
	}

	@Override
	public byte[] get(K key) throws IOException {
		
		return null;
	}

	@Override
	public byte[] delete(K key) throws IOException {
		
		return null;
	}

	@Override
	public ISeekIterator iterator() throws IOException {
		
		return null;
	}

	@Override
	public void close() throws IOException {
		
		
	}

}
