package com.ctriposs.tsdb.level;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.iterator.FileSeekIterator;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.PureFileStorage;
import com.ctriposs.tsdb.table.MemTable;
import com.ctriposs.tsdb.util.FileUtil;

public class PurgeLevel extends Level implements Runnable {

	public final static long MAX_PERIOD = 1000 * 60 * 60 * 24 * 30L;
    public final static long ONE_HOUR = 1000 * 60 * 60L;

	private AtomicLong purgeCounter = new AtomicLong(0);
	private AtomicLong purgeErrorCounter = new AtomicLong(0);

	private ConcurrentSkipListMap<InternalKey, byte[]> dataListMap = new ConcurrentSkipListMap<InternalKey, byte[]>(
			new Comparator<InternalKey>() {
				@Override
				public int compare(InternalKey o1, InternalKey o2) {
					return o1.compareTo(o2);
				}
			});

	public PurgeLevel(FileManager fileManager) {
		super(fileManager);
	}

	public void start() {
		if (!run) {
			run = true;
			//new Thread(this).start();
		}
	}

	public void stop() {
		run = false;
	}

	@Override
	public void run() {

		while (run) {
			try {
				long start = (System.currentTimeMillis() - MAX_PERIOD)
						/ MemTable.MINUTE * MemTable.MINUTE;
				long end = (System.currentTimeMillis() - ONE_HOUR) / MemTable.MINUTE
						* MemTable.MINUTE;

				// Delete too old files
				fileManager.delete(start);
				for (long l = start; l < end; l+= MemTable.MINUTE) {
					Queue<FileMeta> fileMetaQueue = fileManager.getFiles(l);
					if (fileMetaQueue != null && fileMetaQueue.size() >= 2) {
						// Just merge the beginning two meta files
						FileMeta metaOne = fileMetaQueue.peek();
						FileMeta metaTwo = fileMetaQueue.peek();

						String firstFileName = metaOne.getFile().getName();
						String secondFileName = metaTwo.getFile().getName();
						long numberOne = Long
								.valueOf(firstFileName.split("-")[1]);
						long numberTwo = Long
								.valueOf(secondFileName.split("-")[1]);
						IStorage iStorageOne;
						IStorage iStorageTwo;

						if (numberOne < numberTwo) {
							iStorageOne = new PureFileStorage(
									metaOne.getFile(), metaOne.getFile()
											.length());
							iStorageTwo = new PureFileStorage(
									metaTwo.getFile(), metaTwo.getFile()
											.length());
						} else {
							iStorageOne = new PureFileStorage(
									metaTwo.getFile(), metaTwo.getFile()
											.length());
							iStorageTwo = new PureFileStorage(
									metaOne.getFile(), metaOne.getFile()
											.length());
						}

						FileSeekIterator iteratorOne = new FileSeekIterator(
								iStorageOne);
						FileSeekIterator iteratorTwo = new FileSeekIterator(
								iStorageTwo);

						iteratorOne.seekToFirst();
						iteratorTwo.seekToFirst();

						while (iteratorOne.hasNext()) {
							dataListMap.put(iteratorOne.key(),
									iteratorOne.value());
							iteratorOne.next();
						}

						while (iteratorTwo.hasNext()) {
							dataListMap.put(iteratorTwo.key(),
									iteratorTwo.value());
							iteratorTwo.next();
						}

						// Generate new FileMeta
						FileMeta fileMeta = storeFile(l, dataListMap, fileManager.getFileNumber());
						dataListMap.clear();

                        Queue<FileMeta> newFileMetaQueue = fileManager.copy(fileMetaQueue);
                        newFileMetaQueue.remove();
                        FileUtil.forceDelete(metaOne.getFile());
                        newFileMetaQueue.remove();
                        FileUtil.forceDelete(metaTwo.getFile());
                        newFileMetaQueue.add(fileMeta);

						fileManager.put(l, newFileMetaQueue);
					}
				}
				purgeCounter.incrementAndGet();
			} catch (Throwable e) {
				e.printStackTrace();
				purgeErrorCounter.incrementAndGet();
			} finally {
				try {
					Thread.sleep(5 * MemTable.MINUTE);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
