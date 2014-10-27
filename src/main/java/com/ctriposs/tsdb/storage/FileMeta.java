package com.ctriposs.tsdb.storage;


import java.io.File;

import com.ctriposs.tsdb.InternalKey;

public class FileMeta {

    private final File file;

    private final InternalKey smallest;

    private final InternalKey largest;

	public FileMeta(File file, InternalKey smallest, InternalKey largest) {
        this.file = file;
        this.smallest = smallest;
        this.largest = largest;
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
    
	@Override
	public String toString(){
		final StringBuilder sb = new StringBuilder();
		sb.append("FileMeta");
		sb.append("{name=").append(file.getName());
        sb.append(", fileSize=").append(file.length());
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
		sb.append('}');

		return sb.toString();
	}
	
}
