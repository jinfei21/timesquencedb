package com.ctriposs.tsdb.common;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.iterator.FileSeekIterator;
import com.ctriposs.tsdb.iterator.LevelSeekIterator;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;

public abstract class Level {

	public final static int MAX_SIZE = 6;
	public final static long FILE_SIZE = 256 * 1024 * 1024L;
	public final static int THREAD_COUNT = 3;
	
	protected ExecutorService executor = Executors.newFixedThreadPool(2);
	protected Task[] tasks;
	
	protected volatile boolean run = false;
	protected FileManager fileManager;
	protected int level;
	protected long interval;

    protected ConcurrentSkipListMap<Long, Queue<FileMeta>> timeFileMap = new ConcurrentSkipListMap<Long, Queue<FileMeta>>(new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return (int) (o1 - o2);
        }
    });
	
	/** The list change lock. */
	private final Lock lock = new ReentrantLock();

	public Level(FileManager fileManager, int level, long interval, int threads){
		this.fileManager = fileManager;
		this.level = level;
		this.interval = interval;
		this.tasks = new Task[threads];
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
	
	public LevelSeekIterator iterator(){
		return new LevelSeekIterator(fileManager, this, interval);
	}
	
	public void add(long time, FileMeta file) {
		Queue<FileMeta> list = timeFileMap.get(time);
		if(list == null) {
			try{
				lock.lock();
				list = timeFileMap.get(time);
				if(list == null) {
					list = new PriorityQueue<FileMeta>();
					timeFileMap.put(time, list);
				}
			} finally {
				lock.unlock();
			}
		}
		list.add(file);
	}
	
	public Queue<FileMeta> getFiles(long time){
		return timeFileMap.get(format(time));
	}
	
	public int getFileSize(){
		return timeFileMap.size();
	}

	public long format(long time) {
		return time/interval*interval;
	}
	
    public ConcurrentSkipListMap<Long, Queue<FileMeta>> getTimeFileMap() {
        return timeFileMap;
    }
	
    public int getLevelNum(){
    	return level;
    }

	public void delete(long afterTime) throws IOException {
		for(Entry<Long, Queue<FileMeta>> entry : timeFileMap.entrySet()) {
			if(entry.getKey() < afterTime) {
				Queue<FileMeta> list = entry.getValue();
				for(FileMeta meta : list){
					fileManager.delete(meta.getFile());
				}
			}
		}
	}
	
	public byte[] getValueFromFile(InternalKey key)throws IOException{
		long ts = key.getTime();
		Queue<FileMeta> list = getFiles(format(ts));
		if(list != null) {
			for(FileMeta meta : list) {
				if(meta.contains(key)){
					IStorage storage = new PureFileStorage(meta.getFile());
					FileSeekIterator it = new FileSeekIterator(storage);
					it.seekToFirst(key.getCode());

					while(it.hasNext()){
						it.next();
						int diff = fileManager.compare(key,it.key());
						if(0==diff){
							return it.value();
						}else if(diff < 0){
							break;
						}
					}
				}
			}
		}
		return null;
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
					Thread.sleep(500);
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

	public abstract byte[] getValue(InternalKey key) throws IOException;
	
}
