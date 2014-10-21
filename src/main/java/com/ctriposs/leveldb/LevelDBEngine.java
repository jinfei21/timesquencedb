package com.ctriposs.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.leveldb.EngineConfig.LogMode;
import com.ctriposs.leveldb.merge.VersionSet;
import com.ctriposs.leveldb.storage.MapFileLogWriter;
import com.ctriposs.leveldb.storage.PureFileLogWriter;
import com.ctriposs.leveldb.table.BytewiseComparator;
import com.ctriposs.leveldb.table.InternalKeyComparator;
import com.ctriposs.leveldb.table.MemTable;
import com.ctriposs.leveldb.table.TableCache;
import com.google.common.base.Preconditions;

public class LevelDBEngine implements IEngine {
	
    private final File databaseDir;
	private EngineConfig engineConfig;
	private ILogWriter logWriter;
	private MemTable memTable;
	private MemTable immutableMemTable;
	private ExecutorService compactionExecutor;
	
	private final InternalKeyComparator internalKeyComparator;
	private final VersionSet versionSet;
	private final TableCache tableCache = new TableCache();
    private final ReentrantLock mutex = new ReentrantLock();
    private final AtomicBoolean shutDown = new AtomicBoolean();
    
	
	public LevelDBEngine(EngineConfig engineConfig) throws IOException {
		Preconditions.checkNotNull(engineConfig, "engine config is null!");
    	this.engineConfig = engineConfig;
    	this.databaseDir = new File(engineConfig.getStorageDir());
		this.internalKeyComparator = new InternalKeyComparator(new BytewiseComparator());
		this.memTable = new MemTable(internalKeyComparator);
		this.immutableMemTable = null;
		this.compactionExecutor = Executors.newCachedThreadPool();
		
		try{
			mutex.lock();
			versionSet = new VersionSet(databaseDir,tableCache,internalKeyComparator);
			versionSet.recover();
			
		}finally{
			mutex.unlock();
		}
     	
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
