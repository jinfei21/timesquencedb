package com.ctriposs.leveldb.table;

import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;

import com.ctriposs.leveldb.storage.FileName;
import com.google.common.cache.LoadingCache;

public class TableCache {

	private final LoadingCache<Long,TableFile> cache;
	
	
	private static final class TableFile{
		private final Table table;
		
		private TableFile(File databaseDir,long fileNumber){
			File file = new File(databaseDir,FileName.tableFileName(fileNumber));
			FileChannel channel = new FileInputStream(file).getChannel();
			
		}
	}
	
}
