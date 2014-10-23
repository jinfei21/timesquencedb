package com.ctriposs.leveldb.table;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

import com.ctriposs.leveldb.storage.FileName;
import com.ctriposs.leveldb.storage.PureFileTable;
import com.ctriposs.leveldb.util.Finalizer;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;

public class TableCache {

	private final LoadingCache<Long,TableFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<Table>(1);
    
	public TableCache(final File databaseDir,int cacheSize,final UserComparator userComparator){
		Preconditions.checkNotNull(databaseDir,"databasedir is null!");
		this.cache = CacheBuilder.newBuilder()
					 .maximumSize(cacheSize)
					 .removalListener(new RemovalListener<Long,TableFile>() {

						@Override
						public void onRemoval(RemovalNotification<Long,TableFile> notification) {
	                        Table table = notification.getValue().getTable();
	                        finalizer.addCleanup(table, table.closer());
						}
					}).build(new CacheLoader<Long, TableFile>() {

						@Override
						public TableFile load(Long fileNumber) throws Exception {
							
							return new TableFile(databaseDir,fileNumber);
						}
						
					});
	}
	
	private Table getTable(long number){
		Table table;
		try{
			table = cache.get(number).getTable();
		}catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
		return table;
	}
	
	public void close(){
		cache.invalidateAll();
		finalizer.stop();
	}
	
	public void evict(long number){
		cache.invalidate(number);
	}
	
	
	private static final class TableFile{
		private final Table table;
		
		private TableFile(File databaseDir,long fileNumber) throws IOException{
			File file = new File(databaseDir,FileName.tableFileName(fileNumber));
			FileChannel channel = new FileInputStream(file).getChannel();
			try{
			table = new PureFileTable(file.getAbsolutePath(),channel);
			}catch(IOException e){
				Closeables.closeQuietly(channel);
				throw e;
			}
		}
		
		public Table getTable(){
			return table;
		}
	}
	
}
