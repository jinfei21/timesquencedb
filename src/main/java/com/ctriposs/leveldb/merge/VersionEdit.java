package com.ctriposs.leveldb.merge;

import java.util.Map;

import com.ctriposs.leveldb.storage.FileMeta;
import com.ctriposs.leveldb.table.InternalKey;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class VersionEdit {

	private Long logNumber;
	private Long prevLogNumber;
	private Long nextFileNumber;
	private final Map<Integer,InternalKey> compactPointers = Maps.newTreeMap();
	private final Map<Integer,FileMeta> newFiles = Maps.newHashMap();
	private final Map<Integer,Long> deleteFiles = Maps.newHashMap();
	
	public VersionEdit(){
		
	}

	public Long getLogNumber() {
		return logNumber;
	}

	public void setLogNumber(Long logNumber) {
		this.logNumber = logNumber;
	}

	public Long getPrevLogNumber() {
		return prevLogNumber;
	}

	public void setPrevLogNumber(Long prevLogNumber) {
		this.prevLogNumber = prevLogNumber;
	}

	public Long getNextFileNumber() {
		return nextFileNumber;
	}

	public void setNextFileNumber(Long nextFileNumber) {
		this.nextFileNumber = nextFileNumber;
	}

	public Map<Integer, FileMeta> getNewFiles() {
		return newFiles;
	}
	
    public Map<Integer, InternalKey> getCompactPointers()
    {
        return ImmutableMap.copyOf(compactPointers);
    }

    public void setCompactPointer(int level, InternalKey key)
    {
        compactPointers.put(level, key);
    }
    
    public void addFile(int level, FileMeta fileMeta)
    {
        newFiles.put(level, fileMeta);
    }
    
    public void deleteFile(int level, long fileNumber)
    {
        deleteFiles.put(level, fileNumber);
    }
}
