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
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.util.DateFormatter;

public class CompactLevel extends Level {

	public final static long MAX_PERIOD = 1000 * 60 * 60 * 24 * 30L;
    public final static long ONE_HOUR = 1000 * 60 * 60L;
    private static final int MAX_SLEEP_TIME = 5 * 1000;

	private AtomicLong purgeCounter = new AtomicLong(0);
	private AtomicLong purgeErrorCounter = new AtomicLong(0);

	public CompactLevel(FileManager fileManager,int level,long interval) {
		super(fileManager, level,interval);
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

		@Override
		public void process() throws Exception {
            System.out.println("Start running level " + level + " merge thread at " + System.currentTimeMillis());
            System.out.println("Current hash map size at level " + level + " is " + timeFileMap.size());

            if (level == 1 && timeFileMap.firstKey() > DateFormatter.minuteFormatter(System.currentTimeMillis() - ONE_HOUR, level)) {
                try {
                    Thread.sleep(MAX_SLEEP_TIME);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            } else {
                Map<Long, List<Long>> levelMap = new HashMap<Long, List<Long>>();
                NavigableSet<Long> keySet = timeFileMap.descendingKeySet();
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
                        fileMetaList.addAll(timeFileMap.get(time));
                    }

                    mergeSort(higherLevelKey, fileMetaList);
                }
            }
		}

        private void mergeSort(long key, List<FileMeta> fileMetaList) {

        }
		
	}

	@Override
	public byte[] getValue(InternalKey key) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long format(long time) {
		// TODO Auto-generated method stub
		return 0;
	}
}
