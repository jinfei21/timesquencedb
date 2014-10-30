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
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.table.MemTable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class StoreLevel extends Level{

	public final static int MAX_SIZE = 6;
	public final static long FILE_SIZE = 256*1024* 1024L;
	public final static int THREAD_COUNT = 2;
	
	private ExecutorService executor = Executors.newFixedThreadPool(2);
	private Task[] tasks;
	private ArrayBlockingQueue<MemTable> memQueue;
	
	private AtomicLong storeCounter = new AtomicLong(0);
	private AtomicLong storeErrorCounter = new AtomicLong(0);

	public StoreLevel(FileManager fileManager, int threads, int memCount) {
		super(fileManager);
		this.executor = Executors.newFixedThreadPool(threads, new ThreadFactoryBuilder()
															 .setNameFormat("Level0Merger-%d")
															 .setDaemon(true)
															 .build());

		this.tasks = new Task[threads];
		for(int i = 0; i < threads; i++){
			tasks[i] = new Task(i);
		}
		this.memQueue = new ArrayBlockingQueue<MemTable>(memCount);
	}

	public void addMemTable(MemTable memTable) throws Exception {
		this.memQueue.put(memTable);
	}

	public void start() {
		if(!run) {
            run = true;
            for (Task task : tasks) {
                executor.submit(task);
            }
		}
	}

	public void stop() {
		if(run) {
			run = false;
			executor.shutdownNow();
		}
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

	class Task implements Runnable {

		private int num;
		private MemTable table = null;

		public Task(int num) {
			this.num = num;
		}
		
		public byte[] getValue(InternalKey key) {
			if(table != null){
				return table.getValue(key);
			}else{
				return null;
			}
		}

		@Override
		public void run() {
			while(run) {
				try {
					table = memQueue.take();
					storeCounter.incrementAndGet();
					Map<Long,IStorage> storeMap = new HashMap<Long,IStorage>();
					for(Entry<Long, ConcurrentSkipListMap<InternalKey, byte[]>> entry : table.getTable().entrySet()) {
						try{
							fileCount.incrementAndGet();
							FileMeta fileMeta = storeFile(entry.getKey(), entry.getValue(),table.getFileNumber());
							fileManager.add(entry.getKey(), fileMeta);
							fileCount.decrementAndGet();
						}catch(IOException e){
							//TODO
							e.printStackTrace();
						}						
					}
					fileManager.delete(new File(table.getLogFile()));
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
