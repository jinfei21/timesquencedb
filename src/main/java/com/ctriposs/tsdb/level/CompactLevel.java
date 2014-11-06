package com.ctriposs.tsdb.level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
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

		private boolean check() {
            if (level == 2 && (System.currentTimeMillis() - prevLevel.getTimeFileMap().firstKey()) < ONE_HOUR) {
                return true;
            } else if (level > 2) {
                return prevLevel.getTimeFileMap().size() >= 4;
            }

            return false;
		}

		@Override
		public void process() throws Exception {
            System.out.println("Start running level " + level + " merge thread at " + System.currentTimeMillis());
            System.out.println("Current hash map size at level " + level + " is " + timeFileMap.size());

            if (check()) {
                Map<Long, List<Long>> levelMap = new HashMap<Long, List<Long>>();
                NavigableSet<Long> keySet = prevLevel.getTimeFileMap().descendingKeySet();

                for (Long time : keySet) {
                    if (prevLevel.getFiles(time).size() == 0) {
                        try {
                            deleteLock.lock();
                            if (prevLevel.getFiles(time).size() == 0) {
                                prevLevel.getTimeFileMap().remove(time);
                            }
                        } finally {
                            deleteLock.unlock();
                        }
                    } else {
                        long partition = format(time);

                        if (levelMap.containsKey(partition)) {
                            levelMap.get(partition).add(time);
                        } else {
                            List<Long> timeList = new ArrayList<Long>();
                            timeList.add(time);
                            levelMap.put(partition, timeList);
                        }
                    }

                }

                for (Map.Entry<Long, List<Long>> entry : levelMap.entrySet()) {
                    long higherLevelKey = entry.getKey();
                    List<FileMeta> fileMetaList = new ArrayList<FileMeta>();
                    for (Long time : entry.getValue()) {
                        fileMetaList.addAll(prevLevel.getTimeFileMap().get(time));
                    }

                    mergeSort(higherLevelKey, fileMetaList);
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
