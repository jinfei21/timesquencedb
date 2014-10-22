package com.ctriposs.leveldb.merge;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.leveldb.table.InternalKeyComparator;
import com.ctriposs.leveldb.table.TableCache;

public class VersionSet {

	private Version current;
	private AtomicLong nextFileNumber = new AtomicLong(2);
	private long lastSequence;

	public VersionSet(File databaseDir, TableCache tableCache,
			InternalKeyComparator internalKeyComparator) {

	}

	public void recover() {

	}

	public int fileCountInLevel(int level) {
		return current.getFileCountInLevel(level);
	}

	public long getNextFileNumber() {
		return nextFileNumber.getAndIncrement();
	}

	public boolean needCompaction() {
		return true;
	}

	public Version getCurrent() {
		return current;
	}

	public long getLastSequence() {
		return lastSequence;
	}
	
	public void setLastSequence(long lastSequence) {
		this.lastSequence = lastSequence;
	}
}
