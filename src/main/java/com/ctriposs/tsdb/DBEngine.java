package com.ctriposs.tsdb;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.tsdb.iterator.SeekIteratorAdapter;
import com.ctriposs.tsdb.level.PurgeLevel;
import com.ctriposs.tsdb.level.StoreLevel;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.manage.NameManager;
import com.ctriposs.tsdb.table.InternalKeyComparator;
import com.ctriposs.tsdb.table.MemTable;

public class DBEngine implements IDB{

	/** The engine config*/
	private DBConfig config;
	
	/** The memory table for kv*/
	private MemTable memTable;
	
	/** Store memtable to file*/
	private StoreLevel storeLevel;
	
	/** Clean too old files*/
	private PurgeLevel purgeLevel;
	
	/** Manage the file by time sequence */
	private FileManager fileManager;
	
	/** Map name to code for table and column*/
	private NameManager nameManager;
	
	/** The comparator for key*/
	private InternalKeyComparator internalKeyComparator;
	
	/** The memtable change lock. */
	private final Lock lock = new ReentrantLock();
	
	/** The hit counter. */
    private AtomicLong hitCounter = new AtomicLong();

	/** The miss counter. */
	private AtomicLong missCounter = new AtomicLong();

    /** The get counter. */
	private AtomicLong getCounter = new AtomicLong();

    /** The put counter. */
    private AtomicLong putCounter = new AtomicLong();

    /** The delete counter. */
    private AtomicLong deleteCounter = new AtomicLong();
	
    
    
	public DBEngine(DBConfig config) throws IOException{
		this.config = config;
		if(config.getInternalKeyComparator() == null){
			this.internalKeyComparator = new InternalKeyComparator();
		}else{
			this.internalKeyComparator = config.getInternalKeyComparator();
		}
		this.fileManager = new FileManager(config.getDBDir(),config.getFileCapacity(),internalKeyComparator);
		this.nameManager = new NameManager(config.getDBDir());
		this.memTable = new MemTable(config.getDBDir(),fileManager.getFileNumber(),config.getFileCapacity(),config.getMaxMemTable(),internalKeyComparator);
		this.storeLevel = new StoreLevel(fileManager, config.getStoreThread(), config.getMaxMemTable());
		this.purgeLevel = new PurgeLevel(fileManager);
		
		this.storeLevel.start();
		this.purgeLevel.start();
	}
	

	private void checkTime(long time){
		long threshold = time -(System.currentTimeMillis() - config.getMaxPeriod());
		if(threshold < 0){
			throw new IllegalArgumentException("time is to old!");
		}
	}

	@Override
	public void put(String tableName, String colName, long time, byte[] value) throws IOException{
		
		putCounter.incrementAndGet();
		
		checkTime(time);
		
		InternalKey key = new InternalKey(nameManager.getCode(tableName),nameManager.getCode(colName),time);
		
		if(!memTable.add(key, value)){
			try{
				lock.lock();
				if(!memTable.add(key, value)){
					
					try {
						memTable.close();
						storeLevel.addMemTable(memTable);
						memTable = new MemTable(config.getDBDir(),fileManager.getFileNumber(),config.getFileCapacity(),config.getMaxMemTable(),internalKeyComparator);
						memTable.add(key, value);					
					} catch (Exception e) {
						throw new IOException(e);
					}

				}
				
			}finally{
				lock.unlock();
			}
		}
	}
	
	

	@Override
	public byte[] get(String tableName, String colName, long time)throws IOException {
		getCounter.incrementAndGet();
		checkTime(time);
		
		InternalKey key = new InternalKey(nameManager.getCode(tableName),nameManager.getCode(colName),time);
		byte[] value = memTable.getValue(key);
		if(value == null){
			value = storeLevel.getValue(key);
			if(value == null){
				value = fileManager.getValue(key);
			}
		}
		return value;
	}

	@Override
	public void delete(long afterTime)throws IOException{
		deleteCounter.incrementAndGet();
		checkTime(afterTime);
		fileManager.delete(afterTime);
	}
	
	@Override
	public ISeekIterator<InternalKey, byte[]> iterator() {
		
		return new SeekIteratorAdapter(fileManager,nameManager,internalKeyComparator);
	}

	@Override
	public void close() throws IOException {
		
		this.storeLevel.stop();
		this.purgeLevel.stop();
	}

	public long getHitCounter() {
		return hitCounter.get();
	}

	public long getMissCounter() {
		return missCounter.get();
	}

	public long getGetCounter() {
		return getCounter.get();
	}

	public long getPutCounter() {
		return putCounter.get();
	}

	public long getDeleteCounter() {
		return deleteCounter.get();
	}

	public long getStoreCounter(){
		return storeLevel.getStoreCounter();
	}
	
	public long getStoreErrorCounter(){
		return storeLevel.getStoreErrorCounter();
	}
}
