package com.ctriposs.leveldb;

import java.io.File;

import com.ctriposs.leveldb.util.FileUtil;




public class EngineConfig {

	
    private LogMode logMode = LogMode.MapFile;
    private String storageDir;
	
    public EngineConfig(String dir){
        this.storageDir = dir;
		if (!storageDir.endsWith(File.separator)) {
			storageDir += File.separator;
		}
		// validate directory
		if (!FileUtil.isFilenameValid(storageDir)) {
			throw new IllegalArgumentException("Invalid storage data directory : " + storageDir);
		}
    }
    
	public LogMode getLogMode() {
		return logMode;
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
