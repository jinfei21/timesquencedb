package com.ctriposs.leveldb;




public class EngineConfig {

	
    private StorageMode storageMode = StorageMode.PureFile;
    private StartMode startMode = StartMode.ClearOldFile;
    private long maxOffHeapMemorySize = 2 * 1024 * 1024 * 1024L; // Unit: GB
	
    
	public long getMaxOffHeapMemorySize() {
		return this.maxOffHeapMemorySize;
	}   
	
	public StorageMode getStorageMode() {
		return storageMode;
	}

	public EngineConfig setStorageMode(StorageMode storageMode) {
		this.storageMode = storageMode;
		return this;
	}

	public StartMode getStartMode() {
		return startMode;
	}

	public void setStartMode(StartMode startMode) {
		this.startMode = startMode;
	}
	
	public enum StorageMode {
		PureFile,
		MapFile,
		OffHeapFile,
	}
	
	public enum StartMode {
		ClearOldFile,
		RecoveryFromFile
	}
}
