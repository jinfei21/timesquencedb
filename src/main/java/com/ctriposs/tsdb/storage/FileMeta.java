package com.ctriposs.tsdb.storage;


import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.tsdb.InternalKey;

public class FileMeta implements Comparable<FileMeta> {

    private final File file;

    private final InternalKey smallest;

    private final InternalKey largest;
    
    private final long fileNumber;
    
    private final AtomicInteger refCount;

	public FileMeta(long fileNumber,File file, InternalKey smallest, InternalKey largest) {
        this.file = file;
        this.smallest = smallest;
        this.largest = largest;
        this.fileNumber = fileNumber;
        this.refCount = new AtomicInteger(0);
	}

    public InternalKey getSmallest() {
        return smallest;
    }

    public InternalKey getLargest() {
        return largest;
    }

    public File getFile(){
    	return file;
    }
    
    public long getFileNumber(){
    	return fileNumber;
    }

    public boolean contains(InternalKey key) {
        return key.compareTo(smallest) >= 0 && key.compareTo(largest) <= 0;
    }
    
    public int getRefCount(){
    	return this.refCount.get();
    }
    
    public int addRefCount(){
    	return this.refCount.incrementAndGet();
    }
    
    public int decRefCount(){
    	return this.refCount.decrementAndGet();
    }

	@Override
	public String toString(){
		final StringBuilder sb = new StringBuilder();
		sb.append("FileMeta");
		sb.append("{name=").append(file.getName());
        sb.append(", fileSize=").append(file.length());
        sb.append(", fileNumber=").append(fileNumber);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
        sb.append(", refCount=").append(refCount.get());
		sb.append('}');

		return sb.toString();
	}

	@Override
	public int compareTo(FileMeta o) {
		int diff = (int) (o.fileNumber-fileNumber);
		return -diff;
	}
	
}
