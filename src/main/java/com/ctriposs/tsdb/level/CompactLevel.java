package com.ctriposs.tsdb.level;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    public final static long ONE_HOUR = 1000 * 60 * 60L;

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
            System.out.println("Start running level " + level + " merge thread at " + System.currentTimeMillis());
            System.out.println("Current hash map size at level " + level + " is " + timeFileMap.size());

            Map<Long, HashMap<Long, List<FileMeta>>> levelMap = new HashMap<Long, HashMap<Long, List<FileMeta>>>();
            long compactFlag = format(System.currentTimeMillis() - 60 * prevLevel.getLevelInterval(), prevLevel.getLevelInterval());
            ConcurrentNavigableMap<Long, ConcurrentSkipListSet<FileMeta>> headMap = prevLevel.getTimeFileMap().headMap(compactFlag);
            NavigableSet<Long> keySet = headMap.keySet();

            for (Long time : keySet) {
                long ts = format(time, interval);
                if (ts % tasks.length == num) {
                    HashMap<Long, List<FileMeta>> preTimeList = levelMap.get(ts);
                    List<FileMeta> fileMetaList = new ArrayList<FileMeta>();

                    if (preTimeList == null) {
                        preTimeList = new HashMap<Long, List<FileMeta>>();
                        fileMetaList.addAll(prevLevel.getFiles(time));
                        preTimeList.put(time, fileMetaList);
                    } else {
                        fileMetaList.addAll(prevLevel.getFiles(time));
                        preTimeList.put(time, fileMetaList);
                    }
                    levelMap.put(ts, preTimeList);
                }
            }

            for (Map.Entry<Long, HashMap<Long, List<FileMeta>>> entry : levelMap.entrySet()) {
                long higherLevelKey = entry.getKey();
                List<FileMeta> fileMetaList = new ArrayList<FileMeta>();
                for (Map.Entry<Long, List<FileMeta>> e : entry.getValue().entrySet()) {
                    fileMetaList.addAll(e.getValue());
                    // Remove the preLevel file meta
                    prevLevel.getTimeFileMap().remove(e.getKey());
                }

                FileMeta newFileMeta = mergeSort(higherLevelKey, fileMetaList);

                // add to current level
                add(higherLevelKey, newFileMeta);

                // delete the preLevel disk files
                for (FileMeta fileMeta : fileMetaList) {
                    try {
                        FileUtil.forceDelete(fileMeta.getFile());
                    } catch (IOException e) {

                    }
                }
            }
		}

        private FileMeta mergeSort(long time, List<FileMeta> fileMetaList) throws IOException {
        	MergeFileSeekIterator mergeIterator = new MergeFileSeekIterator(fileManager);
            long totalTimeCount = 0;
            long fileLen = 0;
            for (FileMeta meta : fileMetaList) {
                FileSeekIterator fileIterator = new FileSeekIterator(new PureFileStorage(meta.getFile()));
                mergeIterator.addIterator(fileIterator);
                totalTimeCount += fileIterator.timeItemCount();
                fileLen += meta.getFile().length();
            }

            long fileNumber = fileManager.getFileNumber();
            PureFileStorage fileStorage = new PureFileStorage(fileManager.getStoreDir(), time, FileName.dataFileName(fileManager.getFileNumber(), level), fileLen);
            DBWriter dbWriter = new DBWriter(fileStorage, totalTimeCount, fileNumber);
            while (mergeIterator.hasNext()) {
                Map.Entry<InternalKey, byte[]> entry = mergeIterator.next();
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
