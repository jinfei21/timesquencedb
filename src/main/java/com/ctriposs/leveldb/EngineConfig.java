package com.ctriposs.leveldb;

import java.io.File;

import com.ctriposs.leveldb.util.FileUtil;
import com.google.common.base.Preconditions;




public class EngineConfig {

	
    private LogMode logMode = LogMode.MapFile;
    private String storageDir;
    private long maxMemorySize = Constant.DEFAULT_MEMORY_SIZE;
    private int tableCacheSize = Constant.DEFAULT_TABLE_CACHE_SIZE; 
	
    public EngineConfig(String dir){
    	Preconditions.checkNotNull(dir, "storage data directory is null!");
        this.storageDir = dir;
		if (!storageDir.endsWith(File.separator)) {
			storageDir += File.separator;
		}
		// validate directory
		if (!FileUtil.isFilenameValid(storageDir)) {
			throw new IllegalArgumentException("Invalid storage data directory : " + storageDir);
		}
    }
    
    public String getStorageDir(){
    	return this.storageDir;
    }
    
	public LogMode getLogMode() {
		return logMode;
	}
	
	public EngineConfig setMaxMemorySize(long maxMemorySize){
		this.maxMemorySize = maxMemorySize;
		return this;
	}
	
	public long getMaxMemorySize(){
		return maxMemorySize;
	}

	public int getTableCacheSize() {
		return tableCacheSize;
	}

	public EngineConfig setTableCacheSize(int tableCacheSize) {
		this.tableCacheSize = tableCacheSize;
		return this;
	}

	public EngineConfig setStorageMode(LogMode logMode) {
		this.logMode = logMode;
		return this;
	}
	
	public enum LogMode {
		PureFile,
		MapFile
	}
	
}
