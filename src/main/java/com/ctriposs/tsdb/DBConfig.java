package com.ctriposs.tsdb;

import java.io.File;

import com.ctriposs.tsdb.level.PurgeLevel;
import com.ctriposs.tsdb.level.StoreLevel;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.util.FileUtil;
import com.google.common.base.Preconditions;

public class DBConfig {

	private int maxMemInLevel0 = StoreLevel.MAX_SIZE;
	private int maxMemInLevel1 = PurgeLevel.MAX_SIZE;
	private long maxFileSize = FileManager.MAX_FILE_SIZE;
	private int maxFileCount = FileManager.MAX_FILES;
    private int maxMemTableSize;
	private String dir = null;	
	
	public DBConfig(String dir){
    	Preconditions.checkNotNull(dir, "storage data directory is null!");
       
		if (!dir.endsWith(File.separator)) {
			dir += File.separator;
		}
		// validate directory
		if (!FileUtil.isFilenameValid(dir)) {
			throw new IllegalArgumentException("Invalid storage data directory : " + dir);
		}
		this.dir = dir;
	}
	public int getMaxMemInLevel0() {
		return maxMemInLevel0;
	}
	public void setMaxMemInLevel0(int maxMemInLevel0) {
		this.maxMemInLevel0 = maxMemInLevel0;
	}
	public int getMaxMemInLevel1() {
		return maxMemInLevel1;
	}
	public void setMaxMemInLevel1(int maxMemInLevel1) {
		this.maxMemInLevel1 = maxMemInLevel1;
	}
	public long getMaxFileSize() {
		return maxFileSize;
	}
	public void setMaxFileSize(long maxFileSize) {
		this.maxFileSize = maxFileSize;
	}
	public int getMaxFileCount() {
		return maxFileCount;
	}
	public void setMaxFileCount(int maxFileCount) {
		this.maxFileCount = maxFileCount;
	}
	public int getMaxMemTableSize() {
		return maxMemTableSize;
	}
	public void setMaxMemTableSize(int maxMemTableSize) {
		this.maxMemTableSize = maxMemTableSize;
	}

}
