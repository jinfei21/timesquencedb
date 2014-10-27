package com.ctriposs.tsdb.storage;


import com.ctriposs.tsdb.InternalKey;

public class FileMeta {

    private final String fileName;

    private final long fileSize;

    private final InternalKey smallest;

    private final InternalKey largest;

	public FileMeta(String fileName, long fileSize, InternalKey smallest, InternalKey largest) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
	}

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public InternalKey getSmallest() {
        return smallest;
    }

    public InternalKey getLargest() {
        return largest;
    }

	@Override
	public String toString(){
		final StringBuilder sb = new StringBuilder();
		sb.append("FileMeta");
		sb.append("{name=").append(fileName);
        sb.append(", fileSize=").append(fileSize);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
		sb.append('}');

		return sb.toString();
	}
	
}
