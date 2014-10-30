package com.ctriposs.tsdb.level;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.iterator.FileSeekIterator;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.DataMeta;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.FileName;
import com.ctriposs.tsdb.storage.PureFileStorage;
import com.ctriposs.tsdb.table.InternalKeyComparator;
import com.ctriposs.tsdb.table.MemTable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PurgeLevel extends Level implements Runnable {

	public final static long MAX_PERIOD = 1000 * 60 * 60 * 24 * 30L;

    private ConcurrentSkipListMap<InternalKey, byte[]> dataListMap = new ConcurrentSkipListMap<InternalKey, byte[]>(new Comparator<InternalKey>() {
        @Override
        public int compare(InternalKey o1, InternalKey o2) {
            return o1.compare(o1, o2);
        }
    });

	public PurgeLevel(FileManager fileManager) {
        super(fileManager);
	}

	public void start(){
		if(!run){
			run = true;
			new Thread(this).start();
		}
	}
	
	public void stop(){
		run = false;
	}

	@Override
	public void run() {

		while (run) try {
            long start = (System.currentTimeMillis() - MAX_PERIOD) / MemTable.MINUTE * MemTable.MINUTE;
            long end = System.currentTimeMillis() / MemTable.MINUTE * MemTable.MINUTE;

            // Delete too old files
            fileManager.delete(start);
            for (long l = start; l < end; l++) {
                Queue<FileMeta> fileMetaQueue = fileManager.getFiles(l);
                if (fileMetaQueue != null && fileMetaQueue.size() >= 2) {
                    // Just merge the beginning two meta files
                    FileMeta metaOne = fileMetaQueue.poll();
                    FileMeta metaTwo = fileMetaQueue.poll();

                    String firstFileName = metaOne.getFile().getName();
                    String secondFileName = metaTwo.getFile().getName();
                    long numberOne = Long.valueOf(firstFileName.split("-")[1]);
                    long numberTwo = Long.valueOf(secondFileName.split("-")[2]);
                    IStorage iStorageOne;
                    IStorage iStorageTwo;

                    if (numberOne < numberTwo) {
                        iStorageOne = new PureFileStorage(metaOne.getFile(), metaOne.getFile().length());
                        iStorageTwo = new PureFileStorage(metaTwo.getFile(), metaTwo.getFile().length());
                    } else {
                        iStorageOne = new PureFileStorage(metaTwo.getFile(), metaTwo.getFile().length());
                        iStorageTwo = new PureFileStorage(metaOne.getFile(), metaOne.getFile().length());
                    }

                    FileSeekIterator iteratorOne = new FileSeekIterator(iStorageOne, fileManager.getNameManager());
                    FileSeekIterator iteratorTwo = new FileSeekIterator(iStorageTwo, fileManager.getNameManager());

                    iteratorOne.seekToFirst();
                    iteratorTwo.seekToFirst();

                    while (iteratorOne.hasNext()) {
                        dataListMap.put(iteratorOne.key(), iteratorOne.value());
                        iteratorOne.next();
                    }

                    while (iteratorTwo.hasNext()) {
                        dataListMap.put(iteratorTwo.key(), iteratorTwo.value());
                        iteratorTwo.next();
                    }

                    // Write to file
                    storeFile(l, dataListMap, fileManager.getFileNumber());
                    dataListMap.clear();
                }
            }
        } catch (IOException e) {

        }
		
	}
}
