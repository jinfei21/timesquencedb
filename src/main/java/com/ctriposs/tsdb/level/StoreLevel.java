package com.ctriposs.tsdb.level;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IStorage;
import com.ctriposs.tsdb.common.Level;
import com.ctriposs.tsdb.common.MapFileStorage;
import com.ctriposs.tsdb.common.PureFileStorage;
import com.ctriposs.tsdb.iterator.FileSeekIterator;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.DBWriter;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.FileName;
import com.ctriposs.tsdb.table.MemTable;

public class StoreLevel extends Level {


	private ArrayBlockingQueue<MemTable> memQueue;
	protected AtomicInteger fileCount = new AtomicInteger(0);
	private AtomicLong storeCounter = new AtomicLong(0);
	private AtomicLong storeErrorCounter = new AtomicLong(0);

	public StoreLevel(FileManager fileManager, int threads, int memCount,long interval) {
		super(fileManager,0,interval,threads);
		this.memQueue = new ArrayBlockingQueue<MemTable>(memCount);		
		
		for(int i = 0; i < threads; i++){
			tasks[i] = new MemTask(i);
		}

	}

	public void addMemTable(MemTable memTable) throws Exception {
		this.memQueue.put(memTable);
	}

	@Override
	public byte[] getValue(InternalKey key) throws IOException{
		byte[] value = null;

		for(MemTable table : memQueue) {
			value = table.getValue(key);
			if(value != null){
				return value;
			}
		}
		for(Task task: tasks) {
			value = task.getValue(key);
			if(value != null){
				return value;
			}
		}
		
		return getValueFromFile(key);
	}

	class MemTask extends Task {
		
		private MemTable table = null;

		public MemTask(int num) {
			super(num);
		}

		@Override
		public byte[] getValue(InternalKey key) {
			if(table != null){
				return table.getValue(key);
			}else{
				return null;
			}
		}

		@Override
		public void process() throws Exception {
			table = memQueue.take();

			for(Entry<Long, ConcurrentSkipListMap<InternalKey, byte[]>> entry : table.getTable().entrySet()) {
				try{
					fileCount.incrementAndGet();
					FileMeta fileMeta = storeFile(entry.getKey(), entry.getValue(),table.getFileNumber());
					add(entry.getKey(), fileMeta);					
					fileCount.decrementAndGet();
				}catch(IOException e){
					//TODO
					e.printStackTrace();
					storeErrorCounter.incrementAndGet();
				}						
			}
			fileManager.delete(new File(table.getLogFile()));
		
		}

		private FileMeta storeFile(Long time, ConcurrentSkipListMap<InternalKey, byte[]> dataMap, long fileNumber) throws IOException {
			IStorage storage;
			if(fileCount.get() < 8) {
				storage = new MapFileStorage(fileManager.getStoreDir(), time, FileName.dataFileName(fileNumber,level), fileManager.getFileCapacity());
			} else {
				storage = new PureFileStorage(fileManager.getStoreDir(), time, FileName.dataFileName(fileNumber,level), fileManager.getFileCapacity());
			}
			
			int size = dataMap.size();
			DBWriter dbWriter = new DBWriter(storage, size, fileNumber);
			for(Entry<InternalKey, byte[]> entry : dataMap.entrySet()){
				dbWriter.add(entry.getKey(), entry.getValue());
			}	
			
			return dbWriter.close();	
		}

	}
	
	public long getStoreCounter(){
		return storeCounter.get();
	}
	
	public long getStoreErrorCounter(){
		return storeErrorCounter.get();
	}

	@Override
	public void incrementStoreError() {
		storeErrorCounter.incrementAndGet();
		
	}

	@Override
	public void incrementStoreCount() {
		storeCounter.incrementAndGet();
	}

}
