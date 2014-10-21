package com.ctriposs.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.ctriposs.leveldb.EngineConfig.LogMode;
import com.ctriposs.leveldb.storage.MapFileLogWriter;
import com.ctriposs.leveldb.storage.PureFileLogWriter;
import com.ctriposs.leveldb.table.MemTable;
import com.ctriposs.leveldb.util.FileUtil;

public class LevelDBEngine implements IEngine {
	
	private EngineConfig engineConfig;
	private MemTable memTable;
	private MemTable immemTable;
    
	
	public LevelDBEngine(EngineConfig engineConfig) throws IOException {
    	this.engineConfig = engineConfig;

		
		
     	
    }
	
	private ILogWriter createLogWriter(File file,long fileNumber) throws IOException{
		if(engineConfig.getLogMode() == LogMode.MapFile){
			return new MapFileLogWriter(file, fileNumber);
		}else{
			return new PureFileLogWriter(file, fileNumber);
		}
	}

	@Override
	public Iterator<Entry<byte[], byte[]>> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void put(byte[] key, byte[] value) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void put(byte[] key, byte[] value, long ttl) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] get(byte[] key) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] delete(byte[] key) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
