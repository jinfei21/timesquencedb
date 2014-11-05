package com.ctriposs.tsdb.level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
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
import com.ctriposs.tsdb.table.MemTable;
import com.ctriposs.tsdb.util.DateFormatter;

public class CompactLevel extends Level {

	public final static long MAX_PERIOD = 1000 * 60 * 60 * 24 * 30L;
    public final static long ONE_HOUR = 1000 * 60 * 60L;

	private AtomicLong purgeCounter = new AtomicLong(0);
	private AtomicLong purgeErrorCounter = new AtomicLong(0);
	private Level prevLevel;

	public CompactLevel(FileManager fileManager, Level prevLevel, int level, long interval, int threads) {
		super(fileManager, level,interval,threads);
		this.prevLevel = prevLevel;
	}

	@Override
	public void incrementStoreError() {
		purgeErrorCounter.incrementAndGet();
	}

	@Override
	public void incrementStoreCount() {
		purgeCounter.incrementAndGet();
	}

	@Override
	public long getStoreErrorCounter() {
		return purgeErrorCounter.get();
	}

	@Override
	public long getStoreCounter() {
		return purgeCounter.get();
	}

	class CompactTask extends Task {

		public CompactTask(int num) {
			super(num);
			
		}

		@Override
		public byte[] getValue(InternalKey key) {
			return null;
		}

		private boolean check(){
			 if (level == 2 && prevLevel.getTimeFileMap().firstKey() > DateFormatter.minuteFormatter(System.currentTimeMillis() - ONE_HOUR, level)){
				 return false;
			 }else{
				 return true;
			 }
		}
		@Override
		public void process() throws Exception {
            System.out.println("Start running level " + level + " merge thread at " + System.currentTimeMillis());
            System.out.println("Current hash map size at level " + level + " is " + timeFileMap.size());

            if (check()) {

                Map<Long, List<Long>> levelMap = new HashMap<Long, List<Long>>();
                NavigableSet<Long> keySet = prevLevel.getTimeFileMap().descendingKeySet();
                int power = (int) Math.pow(4, (double) (level - 1));

                for (Long time : keySet) {
                    long partition = DateFormatter.minuteFormatter(time, power);

                    if (levelMap.containsKey(partition)) {
                        levelMap.get(partition).add(time);
                    } else {
                        List<Long> timeList = new ArrayList<Long>();
                        timeList.add(time);
                        levelMap.put(partition, timeList);
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

        private void mergeSort(long time, List<FileMeta> fileMetaList) throws IOException {
        	 MergeFileSeekIterator fileSeekIterator = new MergeFileSeekIterator(fileManager);
            long totalTimeCount = 0;
            for (FileMeta meta : fileMetaList) {
                FileSeekIterator fileIterator = new FileSeekIterator(new PureFileStorage(meta.getFile()));
                fileSeekIterator.addIterator(fileIterator);;
                totalTimeCount += fileIterator.timeItemCount();
            }

           
            long fileNumber = fileManager.getFileNumber();
            PureFileStorage fileStorage = new PureFileStorage(fileManager.getStoreDir(), time, FileName.dataFileName(fileNumber, level), MemTable.MAX_MEM_SIZE);
            DBWriter dbWriter = new DBWriter(fileStorage, totalTimeCount, fileNumber);
            while (fileSeekIterator.hasNext()) {
                Map.Entry<InternalKey, byte[]> entry = fileSeekIterator.next();
                dbWriter.add(entry.getKey(), entry.getValue());
            }
        }
		
	}

	@Override
	public byte[] getValue(InternalKey key) throws IOException {
		return getValueFromFile(key);
	}

}
