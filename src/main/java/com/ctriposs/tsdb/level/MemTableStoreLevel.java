package com.ctriposs.tsdb.level;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IStorage;
import com.ctriposs.tsdb.common.Level;
import com.ctriposs.tsdb.common.MapFileStorage;
import com.ctriposs.tsdb.common.PureFileStorage;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.FileName;
import com.ctriposs.tsdb.storage.FilePersistent;
import com.ctriposs.tsdb.storage.Head;
import com.ctriposs.tsdb.storage.TimeItem;
import com.ctriposs.tsdb.table.MemTable;

public class MemTableStoreLevel extends Level {


	private ArrayBlockingQueue<MemTable> memQueue;
	
	private AtomicLong storeCounter = new AtomicLong(0);
	private AtomicLong storeErrorCounter = new AtomicLong(0);

	public MemTableStoreLevel(FileManager fileManager, int threads, int memCount) {
		super(fileManager,0);
		this.memQueue = new ArrayBlockingQueue<MemTable>(memCount);		
		for(int i = 0; i < threads; i++){
			tasks[i] = new MemTask(i);
		}

	}

	public void addMemTable(MemTable memTable) throws Exception {
		this.memQueue.put(memTable);
	}

	
	public byte[] getValue(InternalKey key){
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

		return value;
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
			FilePersistent fPersist = new FilePersistent(storage, size,fileNumber);
			for(Entry<InternalKey, byte[]> entry : dataMap.entrySet()){
				fPersist.add(entry.getKey(), entry.getValue());
			}	
			
			return fPersist.close();	
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
