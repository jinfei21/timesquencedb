package com.ctriposs.tsdb.common;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;

public abstract class Level {

	public final static int MAX_SIZE = 6;
	public final static long FILE_SIZE = 256 * 1024 * 1024L;
	public final static int THREAD_COUNT = 2;
	
	protected ExecutorService executor = Executors.newFixedThreadPool(2);
	protected Task[] tasks;
	
	protected volatile boolean run = false;
	protected static AtomicInteger fileCount = new AtomicInteger(0);
	protected FileManager fileManager;
	protected int level;
	
	protected ConcurrentSkipListMap<Long, Queue<FileMeta>> timeFileMap = new ConcurrentSkipListMap<Long, Queue<FileMeta>>(new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return (int) (o1 - o2);
        }
    });
	
	/** The list change lock. */
	private final Lock lock = new ReentrantLock();

	
	public Level(FileManager fileManager,int level){
		this.fileManager = fileManager;
		this.level = level;
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
	
	public void add(long time, FileMeta file) {
		Queue<FileMeta> list = timeFileMap.get(time);
		if(list == null) {
			try{
				lock.lock();
				list = timeFileMap.get(time);
				if(list == null) {
					list = new PriorityBlockingQueue<FileMeta>();
					timeFileMap.put(time, list);
				}
			} finally {
				lock.unlock();
			}
		}
		list.add(file);
	}
	
	public abstract class Task implements Runnable {

		private int num;

		public Task(int num) {
			this.num = num;
		}
		
		public abstract byte[] getValue(InternalKey key);

		@Override
		public void run() {
			while(run) {
				try {
					incrementStoreCount();
					process();
				} catch (Throwable e) {
					//TODO 
					e.printStackTrace();
					incrementStoreError();
				}
			}
		}
		
		public abstract void process() throws Exception;

	}
	
	public abstract void incrementStoreError();
	
	public abstract void incrementStoreCount();
	
	public abstract long getStoreErrorCounter();
	
	public abstract long getStoreCounter();
	
}
