package com.ctriposs.tsdb.storage;


public class FileMeta {

	private final long number;
	private final long fileSize;
	private final byte[] minKey;
	private final byte[] maxKey;
	
	
	public FileMeta(long number,long fileSize,byte[] minKey,byte[] maxKey){
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


	public byte[] getMinKey() {
		return minKey;
	}


	public byte[] getMaxKey() {
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
