package com.ctriposs.tsdb.manage;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.table.InternalKeyComparator;
import com.ctriposs.tsdb.util.FileUtil;

public class FileManager {

	public final static long MAX_FILE_SIZE = 2*1024*1024*1024L;
	public final static int MAX_FILES = 30; 

	private String dir;
	private AtomicLong maxFileNumber = new AtomicLong(1L); 
	private InternalKeyComparator internalKeyComparator;
    private NameManager nameManager;
   
	public FileManager(String dir,  InternalKeyComparator internalKeyComparator, NameManager nameManager){
		this.dir = dir;
		this.internalKeyComparator = internalKeyComparator;
		this.nameManager = nameManager;
	}
	
	public int compare(InternalKey o1, InternalKey o2){
		return internalKeyComparator.compare(o1,o2);
	}

	public void delete(File file)throws IOException {
		FileUtil.forceDelete(file);
	}
	
	public String getStoreDir(){
		return dir;
	}
	
	public long getFileNumber(){
		return maxFileNumber.incrementAndGet();
	}

    public short getCode(String name) throws IOException {
        return nameManager.getCode(name);
    }

    public String getName(short code) {
        return nameManager.getName(code);
    }

    public InternalKeyComparator getInternalKeyComparator() {
        return internalKeyComparator;
    }
}
