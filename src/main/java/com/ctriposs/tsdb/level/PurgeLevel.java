package com.ctriposs.tsdb.level;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.PureFileStorage;
import com.ctriposs.tsdb.table.InternalKeyComparator;
import com.ctriposs.tsdb.table.MemTable;
import com.ctriposs.tsdb.util.ByteUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class PurgeLevel implements Runnable {

	public final static long MAX_PERIOD = 1000 * 60 * 60 * 24 * 30L;

	private FileManager fileManager;
	private volatile boolean run = false;
	
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

                        IStorage iStorageOne = new PureFileStorage(metaOne.getFile(), metaOne.getFile().length());
                        IStorage iStorageTwo = new PureFileStorage(metaTwo.getFile(), metaTwo.getFile().length());

                        byte[] bytes = new byte[4];
                        iStorageOne.get(0, bytes);
                        int metaSizeOne = ByteUtil.ToInt(bytes);
                        iStorageTwo.get(0, bytes);
                        int metaSizeTwo = ByteUtil.ToInt(bytes);

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
