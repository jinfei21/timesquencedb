package com.ctriposs.tsdb.level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.Level;
import com.ctriposs.tsdb.common.PureFileStorage;
import com.ctriposs.tsdb.iterator.FileSeekIterator;
import com.ctriposs.tsdb.iterator.MergeFileSeekIterator;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.DBWriter;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.FileName;
import com.ctriposs.tsdb.util.FileUtil;

public class CompactLevel extends Level {

	public final static long MAX_PERIOD = 1000 * 60 * 60 * 24 * 30L;

	private AtomicLong storeCounter = new AtomicLong(0);
	private AtomicLong storeErrorCounter = new AtomicLong(0);
	private Level prevLevel;

	public CompactLevel(FileManager fileManager, Level prevLevel, int level, long interval, int threads) {
		super(fileManager, level, interval, threads);
		this.prevLevel = prevLevel;

        for (int i = 0; i < threads; i++) {
            tasks[i] = new CompactTask(i);
        }
	}

	public long getStoreCounter(){
		return storeCounter.get();
	}
	
	public long getStoreErrorCounter(){
		return storeErrorCounter.get();
	}

	@Override
	public void incrementStoreError() {
		storeErrorCounter.incrementAndGet();
	}

	@Override
	public void incrementStoreCount() {
		storeCounter.incrementAndGet();
	}

	class CompactTask extends Task {

		public CompactTask(int num) {
			super(num);
			
		}

		@Override
		public byte[] getValue(InternalKey key) {
			return null;
		}

		@Override
		public void process() throws Exception {

            Map<Long, HashMap<Long, List<FileMeta>>> compactMap = new HashMap<Long, HashMap<Long, List<FileMeta>>>();
            long startTime = format(System.currentTimeMillis() - 5 * prevLevel.getLevelInterval(), prevLevel.getLevelInterval());
            ConcurrentNavigableMap<Long, ConcurrentSkipListSet<FileMeta>> headMap = prevLevel.getTimeFileMap().headMap(startTime);
            NavigableSet<Long> keySet = headMap.keySet();

			for (Long time : keySet) {
				long ts = format(time, interval);
				if (ts % tasks.length == num) {
					HashMap<Long, List<FileMeta>> preTimeList = compactMap.get(ts);
					List<FileMeta> fileMetaList = new ArrayList<FileMeta>();

					if (preTimeList == null) {
						preTimeList = new HashMap<Long, List<FileMeta>>();
					}
					fileMetaList.addAll(prevLevel.getFiles(time));
					preTimeList.put(time, fileMetaList);

					compactMap.put(ts, preTimeList);
				}
			}      


            for (Map.Entry<Long, HashMap<Long, List<FileMeta>>> entry : compactMap.entrySet()) {
                long key = entry.getKey();
                List<FileMeta> fileMetaList = new ArrayList<FileMeta>();
                for (Entry<Long, List<FileMeta>> e : entry.getValue().entrySet()) {
                    fileMetaList.addAll(e.getValue());
                }

                FileMeta newFileMeta = mergeSort(key, fileMetaList);
                
                // add to current level
                add(key, newFileMeta);
                
                // Remove the preLevel file meta               
                for (Entry<Long, List<FileMeta>> e : entry.getValue().entrySet()) {
                     prevLevel.delete(e.getKey(), e.getValue());
                }
                
                // delete the preLevel disk files
                for (FileMeta fileMeta : fileMetaList) {
                    try {
                        FileUtil.forceDelete(fileMeta.getFile());
                    } catch (IOException e) {
                    	e.printStackTrace();
                    	incrementStoreError();
                    }
                }
            }
		}

        private FileMeta mergeSort(long time, List<FileMeta> fileMetaList) throws IOException {
        	MergeFileSeekIterator mergeIterator = new MergeFileSeekIterator(fileManager);
            long totalTimeCount = 0;
            for (FileMeta meta : fileMetaList) {
                FileSeekIterator fileIterator = new FileSeekIterator(new PureFileStorage(meta.getFile()));
                mergeIterator.addIterator(fileIterator);
                totalTimeCount += fileIterator.timeItemCount();
            }

            long fileNumber = fileManager.getFileNumber();
            PureFileStorage fileStorage = new PureFileStorage(fileManager.getStoreDir(), time, FileName.dataFileName(fileNumber, level));
            DBWriter dbWriter = new DBWriter(fileStorage, totalTimeCount, fileNumber);
            while (mergeIterator.hasNext()) {
                Entry<InternalKey, byte[]> entry = mergeIterator.next();
                dbWriter.add(entry.getKey(), entry.getValue());
            }

            return dbWriter.close();
        }
	}

	@Override
	public byte[] getValue(InternalKey key) throws IOException {
		return getValueFromFile(key);
	}

}
