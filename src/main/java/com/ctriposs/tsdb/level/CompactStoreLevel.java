package com.ctriposs.tsdb.level;

import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.Level;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.util.DateFormatter;

public class CompactStoreLevel extends Level {

	public final static long MAX_PERIOD = 1000 * 60 * 60 * 24 * 30L;
    public final static long ONE_HOUR = 1000 * 60 * 60L;

	private AtomicLong purgeCounter = new AtomicLong(0);
	private AtomicLong purgeErrorCounter = new AtomicLong(0);

	public CompactStoreLevel(FileManager fileManager) {
		super(fileManager, 5);
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

            if (level == 1 && timeFileMap.firstKey() > DateFormatter.oneMinuteFormatter(System.currentTimeMillis() - ONE_HOUR)) {

            }
		}
		
	}
}
