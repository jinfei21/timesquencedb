package com.ctriposs.tsdb.manage;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import com.ctriposs.tsdb.storage.FileMeta;

public class FileManager {
	public final static long MAX_FILE_SIZE = 2*1024*1024*1024L;
	public final static int MAX_FILES = 500; 
	
	private int maxFileCount = 0;
	private long maxFileSize = 0;
	
	private ConcurrentHashMap<FileMeta, File> fileMap = new ConcurrentHashMap<FileMeta, File>();	
	public FileManager(int maxFileCount,long maxFileSize){
		this.maxFileCount = maxFileCount;
		this.maxFileSize = maxFileSize;
	}
	
	public void add(FileMeta meta, File file){
		this.fileMap.put(meta, file);
	}
	
	public int getSize(){
		return fileMap.size();
	}
	
}
