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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PurgeLevel implements Runnable {

	public final static long MAX_PERIOD = 1000 * 60 * 60 * 24 * 30L;

	private FileManager fileManager;
	private volatile boolean run = false;
    private ConcurrentSkipListMap<InternalKey, byte[]> dataListMap = new ConcurrentSkipListMap<InternalKey, byte[]>(new Comparator<InternalKey>() {
        @Override
        public int compare(InternalKey o1, InternalKey o2) {
            return o1.compare(o1, o2);
        }
    });
	public PurgeLevel(FileManager fileManager){
		this.fileManager = fileManager;
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

		while (run) {
            try {
                long start = (System.currentTimeMillis() - MAX_PERIOD)/MemTable.MINUTE*MemTable.MINUTE;
                long end = System.currentTimeMillis()/MemTable.MINUTE*MemTable.MINUTE;

                // Delete too old files
                fileManager.delete(start);
                for (long l = start; l < end; l++) {
                    List<FileMeta> fileMetaList = fileManager.getFiles(l);
                    if (fileMetaList != null && fileMetaList.size() >= 2) {
                        // Just merge the beginning two meta files
                        FileMeta metaOne = fileMetaList.get(0);
                        FileMeta metaTwo = fileMetaList.get(1);

                        String firstFileName = metaOne.getFile().getName();
                        String secondFileName = metaTwo.getFile().getName();
                        long numberOne = Long.valueOf(firstFileName.split("-")[1]);
                        long numberTwo = Long.valueOf(secondFileName.split("-")[2]);
                        IStorage iStorageOne = null;
                        IStorage iStorageTwo = null;

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
                    }
                }
            } catch (IOException e) {

            }
        }
		
	}

    private void mergeList(List<FileMeta> fileMetaList) {
        // first sort the list
        InternalKeyComparator keyComparator = new InternalKeyComparator();
        Arrays.sort(fileMetaList.toArray(new FileMeta[]{}), new FileMetaComparator(keyComparator));

        for (int i = 0; i < fileMetaList.size() - 1; i++) {
            int j = i + 1;
            if (keyComparator.compare(fileMetaList.get(j).getSmallest(), fileMetaList.get(i).getLargest()) > 0) {

            }
        }
    }

    private static class FileMetaComparator implements Comparator<FileMeta> {

        private final InternalKeyComparator internalKeyComparator;

        public FileMetaComparator(InternalKeyComparator keyComparator) {
            this.internalKeyComparator = keyComparator;
        }

        @Override
        public int compare(FileMeta m1, FileMeta m2) {
            return this.internalKeyComparator.compare(m1.getSmallest(), m2.getSmallest());
        }
    }
}
