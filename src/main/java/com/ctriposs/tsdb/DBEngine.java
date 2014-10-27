package com.ctriposs.tsdb;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.tsdb.level.PurgeLevel;
import com.ctriposs.tsdb.level.StoreLevel;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.manage.NameManager;
import com.ctriposs.tsdb.table.InternalKeyComparator;
import com.ctriposs.tsdb.table.MemTable;

public class DBEngine implements IDB {

	private DBConfig config;
	
	private MemTable memTable;
	
	private StoreLevel storeLevel;
	
	private PurgeLevel purgeLevel;
	
	private FileManager fileManager;
	
	private NameManager nameManager;
	
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
	
	public DBEngine(DBConfig config){
		this.config = config;
		this.internalKeyComparator = new InternalKeyComparator();
		this.fileManager = new FileManager(config.getDBDir(), 0);
		this.nameManager = new NameManager(config.getDBDir());
		this.memTable = new MemTable(config.getMaxMemTable(),internalKeyComparator);
		this.storeLevel = new StoreLevel(fileManager, config.getStoreThread(), config.getMaxMemTable());
		this.purgeLevel = new PurgeLevel(fileManager);
		
		this.storeLevel.start();
		this.purgeLevel.start();
	}
	
	private long format(long time){
		return time/1000*1000;
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
		
		InternalKey key = new InternalKey(nameManager.getTableCode(tableName),nameManager.getColumnCode(colName),format(time));
		
		if(!memTable.add(key, value)){
			try{
				lock.lock();
				if(!memTable.add(key, value)){
					
					try {
						storeLevel.addMemTable(memTable);
					} catch (Exception e) {
						throw new IOException(e);
					}
					memTable = new MemTable(config.getMaxMemTable(),internalKeyComparator);
					memTable.add(key, value);
				}
				
			}finally{
				lock.unlock();
			}
		}
	}
	
	

	@Override
	public byte[] get(String tableName, String colName, long time) {
		getCounter.incrementAndGet();
		checkTime(time);
		
		InternalKey key = new InternalKey(nameManager.getTableCode(tableName),nameManager.getColumnCode(colName),format(time));
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
	public void delete(long afterTime) {
		deleteCounter.incrementAndGet();
		checkTime(afterTime);
		fileManager.delete(format(afterTime));
	}
	
	@Override
	public ISeekIterator<InternalKey, byte[]> iterator() {
		
		return null;
	}

	@Override
	public void close() throws IOException {
		
		this.storeLevel.stop();
		this.purgeLevel.stop();
	}

	public AtomicLong getHitCounter() {
		return hitCounter;
	}

	public AtomicLong getMissCounter() {
		return missCounter;
	}

	public AtomicLong getGetCounter() {
		return getCounter;
	}

	public AtomicLong getPutCounter() {
		return putCounter;
	}

	public AtomicLong getDeleteCounter() {
		return deleteCounter;
	}

}
