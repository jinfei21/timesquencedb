package com.ctriposs.tsdb.level;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.DataMeta;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.FileName;
import com.ctriposs.tsdb.storage.MapFileStorage;
import com.ctriposs.tsdb.storage.PureFileStorage;
import com.ctriposs.tsdb.table.MemTable;
import com.ctriposs.tsdb.util.ByteUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class StoreLevel {
	public final static int MAX_SIZE = 6;
	public final static long FILE_SIZE = 256*1024*1024;
	public final static int MAX_MEM_SIZE = 6;
	public final static int THREAD_COUNT = 2;
	
	private ExecutorService executor = Executors.newFixedThreadPool(2);
	private Task[] tasks;
	private volatile boolean run = false;
	private FileManager fileManager;
	private ArrayBlockingQueue<MemTable> memQueue;
	
	private AtomicInteger fileCount = new AtomicInteger(0);
	
	private AtomicLong storeCounter = new AtomicLong(0);
	private AtomicLong storeErrorCounter = new AtomicLong(0);

	public StoreLevel(FileManager fileManager,int threads,int memCount) {
		
		this.executor = Executors.newFixedThreadPool(threads, new ThreadFactoryBuilder()
															 .setNameFormat("Level0Merger-%d")
															 .setDaemon(true)
															 .build());

		this.tasks = new Task[threads];
		for(int i=0;i<threads;i++){
			tasks[i] = new Task(i);
		}
		this.memQueue = new ArrayBlockingQueue<MemTable>(memCount);
		this.fileManager = fileManager;
	}

	public void addMemTable(MemTable memTable) throws Exception {
		
		this.memQueue.put(memTable);
	}

	public void start() {

		if(!run){
			
			for (int i = 0; i < tasks.length; i++) {
				executor.submit(tasks[i]);
			}
			run = true;
		}
	}

	public void stop() {
		if(run){
			run = false;
			executor.shutdownNow();
		}
	}
	
	public byte[] getValue(InternalKey key){
		byte[] value = null;
		for(MemTable table:memQueue){
			value = table.getValue(key);
			if(value != null){
				return value;
			}
		}
		for(Task task:tasks){
			value = task.getValue(key);
		}
		return value;
	}

	class Task implements Runnable {

		private int num;
		private MemTable table = null;
		public Task(int num) {
			this.num = num;
		}
		
		public byte[] getValue(InternalKey key){
			if(table != null){
				return table.getValue(key);
			}else{
				return null;
			}
		}
		
		private FileMeta storeFile(Long time,ConcurrentSkipListMap<InternalKey, byte[]> dataMap)throws IOException{
			
			IStorage storage = null;
			if(fileCount.get() < 8){
				storage = new MapFileStorage(fileManager.getStoreDir(),time,FileName.dataFileName(fileManager.getFileNumber()), fileManager.getFileCapacity());
			}else{
				storage = new PureFileStorage(fileManager.getStoreDir(),time,FileName.dataFileName(fileManager.getFileNumber()), fileManager.getFileCapacity());
			}
			
			int size = dataMap.size();
			int dataOffset = 4 + DataMeta.META_SIZE*size;

			storage.put(0, ByteUtil.toBytes(size));
			int i=0;
			InternalKey smallest = null;
			InternalKey largest = null;
			for(Entry<InternalKey, byte[]> entry:dataMap.entrySet()){
				if(i==0){
					smallest = entry.getKey();
				}
				//write meta
				int metaOffset = 4+DataMeta.META_SIZE*i;
				storage.put(metaOffset + DataMeta.CODE_OFFSET, ByteUtil.toBytes(entry.getKey().getCode()));
				storage.put(metaOffset + DataMeta.TIME_OFFSET, ByteUtil.toBytes(entry.getKey().getTime()));
				storage.put(metaOffset + DataMeta.VALUE_SIZE_OFFSET, ByteUtil.toBytes(entry.getValue().length));
				storage.put(metaOffset + DataMeta.VALUE_OFFSET_OFFSET, ByteUtil.toBytes(dataOffset));
				
				//write data
				storage.put(dataOffset, entry.getValue());
				dataOffset += entry.getValue().length;
				i++;
				largest =  entry.getKey();
			}
			FileMeta fileMeta = new FileMeta(new File(storage.getName()), smallest, largest);
			return fileMeta;	
		}

		@Override
		public void run() {
			while(run){
				try {
					table = memQueue.take();
					storeCounter.incrementAndGet();
					Map<Long,IStorage> storeMap = new HashMap<Long,IStorage>();
					for(Entry<Long,ConcurrentSkipListMap<InternalKey, byte[]>>entry:table.getTable().entrySet()){
						try{
							fileCount.incrementAndGet();
							FileMeta fileMeta = storeFile(entry.getKey(),entry.getValue());
							fileManager.add(entry.getKey(), fileMeta);
							fileCount.decrementAndGet();
						}catch(IOException e){
							//TODO
							e.printStackTrace();
						}						
					}
					
				} catch (Throwable e) {
					//TODO 
					e.printStackTrace();
					storeErrorCounter.incrementAndGet();
				}
			}
		}

	}
	
	public long getStoreCounter(){
		return storeCounter.get();
	}
	
	public long getStoreErrorCounter(){
		return storeErrorCounter.get();
	}
	
}
