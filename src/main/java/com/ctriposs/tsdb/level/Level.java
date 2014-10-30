package com.ctriposs.tsdb.level;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.FileName;
import com.ctriposs.tsdb.storage.IndexHead;
import com.ctriposs.tsdb.storage.IndexMeta;
import com.ctriposs.tsdb.storage.MapFileStorage;
import com.ctriposs.tsdb.storage.PureFileStorage;

public abstract class Level {

	protected volatile boolean run = false;
	protected static AtomicInteger fileCount = new AtomicInteger(0);
	protected FileManager fileManager;

	
	public Level(FileManager fileManager){
		this.fileManager = fileManager;
		
	}
	
	protected FileMeta storeFile(Long time, ConcurrentSkipListMap<InternalKey, byte[]> dataMap, long fileNumber) throws IOException {
		
		IStorage storage;
		if(fileCount.get() < 8) {
			storage = new MapFileStorage(fileManager.getStoreDir(), time, FileName.dataFileName(fileNumber), fileManager.getFileCapacity());
		} else {
			storage = new PureFileStorage(fileManager.getStoreDir(), time, FileName.dataFileName(fileNumber), fileManager.getFileCapacity());
		}
		
		int size = dataMap.size();
		int dataOffset = 4 + IndexMeta.META_SIZE * size;
		
		int i = 0;
		InternalKey smallest = null;
		InternalKey largest = null;
		for(Entry<InternalKey, byte[]> entry : dataMap.entrySet()){
			if (i == 0) {
				smallest = entry.getKey();
			}
			// write meta
			int metaOffset = IndexHead.HEAD_SIZE + IndexMeta.META_SIZE * i;			
			storage.put(metaOffset, entry.getKey().toByte(entry.getValue().length, dataOffset));
			// write data
			storage.put(dataOffset, entry.getValue());
			dataOffset += entry.getValue().length;
			i++;
			largest =  entry.getKey();
		}	
		//write head
		IndexHead head = new IndexHead(size,smallest,largest);
		storage.put(0, head.toByte());
		storage.close();			

        return new FileMeta(fileNumber,new File(storage.getName()), smallest, largest);
	}
	
}
