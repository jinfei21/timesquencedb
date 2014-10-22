package com.ctriposs.leveldb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.leveldb.EngineConfig.LogMode;
import com.ctriposs.leveldb.merge.Version;
import com.ctriposs.leveldb.merge.VersionEdit;
import com.ctriposs.leveldb.merge.VersionSet;
import com.ctriposs.leveldb.storage.FileMeta;
import com.ctriposs.leveldb.storage.FileName;
import com.ctriposs.leveldb.storage.MapFileLogWriter;
import com.ctriposs.leveldb.storage.PureFileLogWriter;
import com.ctriposs.leveldb.storage.TableBuilder;
import com.ctriposs.leveldb.table.BytewiseComparator;
import com.ctriposs.leveldb.table.InternalKey;
import com.ctriposs.leveldb.table.InternalKeyComparator;
import com.ctriposs.leveldb.table.MemTable;
import com.ctriposs.leveldb.table.Slice;
import com.ctriposs.leveldb.table.TableCache;
import com.ctriposs.leveldb.table.ValueType;
import com.google.common.base.Preconditions;

public class LevelDBEngine implements IEngine {
	
    private final File databaseDir;
	private EngineConfig engineConfig;
	private ILogWriter logWriter;
	private MemTable memTable;
	private MemTable immutableMemTable;
	private ExecutorService compactionExecutor;
	private Future<?> backgroundCompaction;
    private volatile Throwable backgroundException;
	
	private final InternalKeyComparator internalKeyComparator;
	private final VersionSet versionSet;
	private final TableCache tableCache = new TableCache();
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition backgroundCondition = mutex.newCondition();
   
    private final AtomicBoolean shutDown = new AtomicBoolean(false);
    
	
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
			
			//从文件中恢复
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
	public ISeekIterator<byte[], byte[]> iterator() {
		// TODO Auto-generated method stub
		
		return null;
	}
	
    public void checkBackgroundException() {
        Throwable e = backgroundException;
        if(e!=null) {
            throw new RuntimeException(e);
        }
    }
	
	private void maybeScheduleCompaction(){
		Preconditions.checkState(mutex.isHeldByCurrentThread());
		if(backgroundCompaction != null){
			
		}else if(shutDown.get()){
			
		}else if(immutableMemTable==null &&
				!versionSet.needCompaction()){
			
		}else{
			backgroundCompaction = compactionExecutor.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
                    try {
                        backgroundCall();
                    }catch (Throwable e) {
                        backgroundException = e;
                    }
					return null;
				}
				
			});
		}
		
	}
	
	private void backgroundCall() throws IOException{
		try{
			mutex.lock();
			
			if(backgroundCompaction == null){
				return;
			}
			
			try{
				if(!shutDown.get()){
					backgroundCompaction();
				}
			}finally{
				backgroundCompaction = null;
			}
			
			maybeScheduleCompaction();
		}finally{
			try{
				backgroundCondition.signalAll();
			}finally{
				mutex.unlock();
			}
		}
	}
	
    private void backgroundCompaction()throws IOException{
    	Preconditions.checkState(mutex.isHeldByCurrentThread());
    	compactMemTable();
    	
    	
    }
    
    private void compactMemTable()throws IOException{
    	Preconditions.checkState(mutex.isHeldByCurrentThread());
    	if(immutableMemTable == null){
    		return;
    	}
    	
    	try{
	    	VersionEdit edit = new VersionEdit();
	    	Version base = versionSet.getCurrent();
	    	
	    	WriteMemTableToLevel0(immutableMemTable,edit,base);
	    	
	    	if(shutDown.get()){
	    		throw new IOException("The engine shutdown during memtabl compaction!");
	    	}
	    	
	    	edit.setPrevLogNumber(0L);
	    	edit.setLogNumber(logWriter.getFileNumber());
	    	
	    	//versionSet.logAndApply(edit);
	    	
	    	immutableMemTable = null;
	    	
	    	//deleteObsoleteFiles();
    	}finally{
    		backgroundCondition.signalAll();
    	}
    	
    }
    
    private void WriteMemTableToLevel0(MemTable table,VersionEdit edit,Version base) throws IOException{
    	Preconditions.checkState(mutex.isHeldByCurrentThread());
    	long fileNumber = versionSet.getNextFileNumber();
    	
    	FileMeta meta;
    	try{
    		mutex.unlock();
    		meta = buildTable(table,fileNumber);
    	}finally{
    		mutex.lock();
    	}
    	
    }
    
    private FileMeta buildTable(MemTable table,long fileNumber) throws IOException{
    	File file = new File(databaseDir,FileName.tableFileName(fileNumber));
    	try{
    		TableBuilder tableBuilder = new TableBuilder(file);
    		InternalKey minKey = null;
    		InternalKey maxKey = null;
    		
    		ISeekIterator<InternalKey, Slice> it = table.iterator();   
    		while(it.hasNext()){
    			Entry<InternalKey, Slice> entry = it.next();
    			InternalKey key = entry.getKey();
    			if(minKey == null){
    				minKey = key;
    			}else{
    				assert (internalKeyComparator.compare(key, maxKey) > 0) : "key must be greater than last key";
    			}
    			maxKey = key;
    			tableBuilder.add(key.encode(), entry.getValue());
    		}
    		tableBuilder.finish();
    		if(minKey == null){
    			return null;
    		}
    		FileMeta fileMeta = new FileMeta(fileNumber,file.length(), minKey, maxKey);
    		
    		
    		return fileMeta;
    	}catch (IOException e) {
            file.delete();
            throw e;
        }
    }
    
    private void deleteObsoleteFiles(){
    	Preconditions.checkState(mutex.isHeldByCurrentThread());
    	
    }
	
	private void makeRoomForWrite(boolean force){
		Preconditions.checkState(mutex.isHeldByCurrentThread());
		
		boolean allowDelay = !force;
		
		while(true){
			if(allowDelay && versionSet.fileCountInLevel(0) > Constant.LEVEL0_SLOWDOWN_WRITES_THRESHOLD){
				try{
					mutex.unlock();
					Thread.sleep(1);
				}catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    mutex.lock();
                }
				
				allowDelay = false;
			}else if(!force && memTable.getUsed()<engineConfig.getMaxMemorySize()){
				break;
			}else if(immutableMemTable != null){
				backgroundCondition.awaitUninterruptibly();
			}else if(versionSet.fileCountInLevel(0) >= Constant.LEVEL0_STOP_WRITES_THRESHOLD){
				backgroundCondition.awaitUninterruptibly();
			}else{
				try{
					logWriter.close();
				}catch(IOException e){
					throw new RuntimeException("Unable to close log file "+logWriter.getFile(), e);
				}
				
				long logFileNumber = versionSet.getNextFileNumber();
				
				try{
					this.logWriter = createLogWriter(new File(databaseDir,FileName.logFileName(logFileNumber)), logFileNumber);
				}catch(IOException e){
                    throw new RuntimeException("Unable to open new log file " + new File(databaseDir, FileName.logFileName(logFileNumber)).getAbsoluteFile(), e);
				}
				immutableMemTable = memTable;
				memTable = new MemTable(internalKeyComparator);
				force = false;
				
			}
			
		}
		
	}

	@Override
	public void put(byte[] key, byte[] value) throws IOException {
		writeInternal(new Slice(key),new Slice(value));
	}
	
	private void writeInternal(Slice key,Slice value)throws IOException {
		checkBackgroundException();
		try{
			mutex.lock();
			makeRoomForWrite(false);
			final long sequence = versionSet.getLastSequence() + 1;
			versionSet.setLastSequence(sequence);
			try{
				logWriter.addRecord(key,value, true);
			}catch(IOException e) {
                throw e;
            }
			memTable.add(sequence, ValueType.VALUE, key, value);
		}finally{
			mutex.unlock();
		}	
	}

	@Override
	public byte[] get(byte[] key) throws IOException {
		
		return null;
	}

	@Override
	public byte[] delete(byte[] key) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
        if (shutDown.getAndSet(true)) {
            return;
        }
        
        try{
        	mutex.lock();
        	
        	while(backgroundCompaction != null){
        		backgroundCondition.awaitUninterruptibly();
        	}
        }finally{
        	mutex.unlock();
        }
        
        compactionExecutor.shutdown();
		
	}
}
