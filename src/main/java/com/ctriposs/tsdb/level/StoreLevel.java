package com.ctriposs.tsdb.level;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ctriposs.tsdb.table.MemTable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class StoreLevel {
	public final static int MAX_SIZE = 6;

	private ExecutorService executor = Executors.newFixedThreadPool(2);
	private Task[] tasks;
	private volatile boolean run = false;
	private PurgeLevel level1Merger;
	private ArrayBlockingQueue<MemTable> memQueue;

	public StoreLevel(PurgeLevel level1Merger, int threads,int memSize) {
		
		this.executor = Executors.newFixedThreadPool(threads, new ThreadFactoryBuilder()
															 .setNameFormat("Level0Merger-%d")
															 .setDaemon(true)
															 .build());
		this.level1Merger = level1Merger;
		this.tasks = new Task[threads];
		for(int i=0;i<threads;i++){
			tasks[i] = new Task(i);
		}
		this.memQueue = new ArrayBlockingQueue<MemTable>(memSize);
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


	class Task implements Runnable {

		private int num;

		public Task(int num) {
			this.num = num;
		}

		@Override
		public void run() {
			while(run){
				try {
					MemTable table1 = memQueue.take();
					MemTable table2 = memQueue.take();
					
				} catch (InterruptedException e) {
					//TODO 
					e.printStackTrace();
				}
			}
		}


	}
}
