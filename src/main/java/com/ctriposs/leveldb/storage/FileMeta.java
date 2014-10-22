package com.ctriposs.leveldb.storage;

import com.ctriposs.leveldb.table.InternalKey;

public class FileMeta {

	private final long number;
	private final long fileSize;
	private final InternalKey minKey;
	private final InternalKey maxKey;
	
	
	public FileMeta(long number,long fileSize,InternalKey minKey,InternalKey maxKey){
		this.number = number;
		this.fileSize = fileSize;
		this.minKey = minKey;
		this.maxKey = maxKey;
	}
	
	
	public long getNumber(){
		return number;
	}


	public long getFileSize() {
		return fileSize;
	}


	public InternalKey getMinKey() {
		return minKey;
	}


	public InternalKey getMaxKey() {
		return maxKey;
	}

	@Override
	public String toString(){
		final StringBuilder sb = new StringBuilder();
		sb.append("FileMeta");
		sb.append("{number=").append(number);
		sb.append(",fileSize=").append(fileSize);
		sb.append(",minKey=").append(minKey);
		sb.append(",maxKey=").append(maxKey);
		sb.append('}');
		return sb.toString();
	}
	
}
