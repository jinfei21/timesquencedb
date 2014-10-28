package com.ctriposs.tsdb.manage;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.iterator.FileSeekInterator;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.PureFileStorage;
import com.ctriposs.tsdb.table.InternalKeyComparator;
import com.ctriposs.tsdb.table.MemTable;
import com.ctriposs.tsdb.util.FileUtil;

public class FileManager {
	public final static long MAX_FILE_SIZE = 2*1024*1024*1024L;
	public final static int MAX_FILES = 30; 

	private ConcurrentSkipListMap<Long, List<FileMeta>> timeFileMap = new ConcurrentSkipListMap<Long, List<FileMeta>>(new Comparator<Long>() {

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
	private InternalKeyComparator internalKeyComparator;
	private NameManager nameManager;
	
	public FileManager(String dir, long fileCapacity, InternalKeyComparator internalKeyComparator,NameManager nameManager){
		this.dir = dir;
		this.fileCapacity = fileCapacity;
		this.internalKeyComparator = internalKeyComparator;
		this.nameManager = nameManager;
	}
	
	public void add(long time, FileMeta file) {
		List<FileMeta> list = timeFileMap.get(time);
		if(list == null) {
			try{
				lock.lock();
				list = timeFileMap.get(time);
				if(list == null) {
					list = new Vector<FileMeta>();
					timeFileMap.put(time, list);
				}
			} finally {
				lock.unlock();
			}
		}
		list.add(file);
	}
	
	public List<FileMeta> getFiles(long time){
		return timeFileMap.get(time);
	}
	
	public int getSize(){
		return timeFileMap.size();
	}
	
	private long format(long time){
		return time/ MemTable.MINUTE*MemTable.MINUTE;
	}
	
	public byte[] getValue(InternalKey key) throws IOException{
		long ts = key.getTime();
		List<FileMeta> list = getFiles(format(ts));
		if(list != null){
			for(FileMeta meta : list){
				if(meta.contains(key)){
					IStorage storage = new PureFileStorage(meta.getFile(), meta.getFile().length());
					FileSeekInterator it = new FileSeekInterator(storage, nameManager);
					it.seek(key.getCode());
					while(it.hasNext()){
						if(0==internalKeyComparator.compare(key,it.key())){
							return it.value();
						}
						it.next();
					}
				}
			}
		}

		return null;
	}
	
	public void delete(long afterTime) throws IOException {
		
		for(Entry<Long, List<FileMeta>> entry : timeFileMap.entrySet()) {
			if(entry.getKey() < afterTime) {
				List<FileMeta> list = entry.getValue();
				for(FileMeta meta : list){
					FileUtil.forceDelete(meta.getFile());
				}
			}
		}
	}
	
	public void delete(File file)throws IOException {
		FileUtil.forceDelete(file);
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
