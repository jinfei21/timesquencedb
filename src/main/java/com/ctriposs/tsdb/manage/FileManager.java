package com.ctriposs.tsdb.manage;

import java.util.Comparator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.storage.FileMeta;

public class FileManager {
	public final static long MAX_FILE_SIZE = 2*1024*1024*1024L;
	public final static int MAX_FILES = 30; 

    private ConcurrentSkipListMap<Long, List<FileMeta>> timeFileMap = new ConcurrentSkipListMap<Long, List<FileMeta>>(
        new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return (int)(o1.longValue() - o2.longValue());
        }
    });
	
	/** The list change lock. */
	private final Lock lock = new ReentrantLock();
	
	private String dir;
	private long fileCapacity;
	private AtomicLong maxFileNumber = new AtomicLong(0L); 
	
	
	public FileManager(String dir,long fileCapacity){
		this.dir = dir;
		this.fileCapacity = fileCapacity;
	}
	
	public void add(long time, FileMeta file){
		List<FileMeta> list = timeFileMap.get(time);
		if(list == null){
			try{
				lock.lock();
				list = timeFileMap.get(time);
				if(list==null){
					list = new Vector<FileMeta>();
				}
			}finally{
				lock.unlock();
			}
		}
		list.add(file);
		timeFileMap.put(time, list);
	}
	
	public List<FileMeta> getFiles(long time){
		return timeFileMap.get(time);
	}
	
	public int getSize(){
		return timeFileMap.size();
	}
	
	public byte[] getValue(InternalKey key){
		return null;
	}
	
	public void delete(long afterTime){
		
	}
	
	public String getStoreDir(){
		return dir;
	}

	public long getFileCapacity() {
		return fileCapacity;
	}
	
	public long getFileNumber(){
		return maxFileNumber.incrementAndGet();
	}
}
