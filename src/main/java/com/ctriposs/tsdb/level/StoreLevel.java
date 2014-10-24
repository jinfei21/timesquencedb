package com.ctriposs.tsdb.level;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.MapFileStorage;
import com.ctriposs.tsdb.storage.PureFileStorage;
import com.ctriposs.tsdb.table.MemTable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class StoreLevel {
	public final static int MAX_SIZE = 6;
	public final static int MAX_MEM_SIZE = 6;
	public final static int THREAD_COUNT = 2;
	
	private ExecutorService executor = Executors.newFixedThreadPool(2);
	private Task[] tasks;
	private volatile boolean run = false;
	private FileManager fileManager;
	private ArrayBlockingQueue<MemTable> memQueue;
	private AtomicInteger fileCount = new AtomicInteger(0);

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

		@Override
		public void run() {
			while(run){
				try {
					table = memQueue.take();
					
					Map<Long,IStorage> storeMap = new HashMap<Long,IStorage>();
					for(Entry<Long,AtomicInteger> entry:table.timeMap().entrySet()){
						int fCount = fileCount.incrementAndGet();
						if(fCount<8){
							
							try {
								storeMap.put(entry.getKey(), new MapFileStorage(fileManager.getStoreDir(), entry.getKey(),fCount, fileManager.getFileCapacity()));
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}else{
							try {
								storeMap.put(entry.getKey(), new PureFileStorage(fileManager.getStoreDir(), entry.getKey(),fCount, fileManager.getFileCapacity()));
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					for(Entry<InternalKey,byte[]> entry:table.entrySet()){
						
					}
					
				} catch (InterruptedException e) {
					//TODO 
					e.printStackTrace();
				}
			}
		}


	}
}
