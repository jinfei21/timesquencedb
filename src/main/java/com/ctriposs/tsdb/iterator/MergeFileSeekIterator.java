package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListSet;

import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IFileIterator;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.CodeItem;

public class MergeFileSeekIterator{
	
	private FileManager fileManager;
	private ConcurrentSkipListSet<IFileIterator<InternalKey, byte[]>> itSet;
	private Direction direction;
	private Entry<InternalKey, byte[]> curEntry;
	private IFileIterator<InternalKey, byte[]> curIt;
	private long curSeekTime;
	private InternalKey seekKey;
	
	public MergeFileSeekIterator(FileManager fileManager, IFileIterator<InternalKey, byte[]>... its) {
		this.fileManager = fileManager;
		this.itSet = new ConcurrentSkipListSet<IFileIterator<InternalKey,byte[]>>(fileManager.getFileIteratorComparator());
		addIterator(its);
		this.direction = Direction.forward;
		this.curEntry = null;
		this.curIt = null;
	}
	
	public void addIterator(IFileIterator<InternalKey, byte[]>... its) {
		for(IFileIterator<InternalKey, byte[]> it : its) {
            itSet.add(it);
		}
	}

	public boolean hasNext() {

		boolean result = false;

		if(itSet != null) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				if(it.hasNext()) {
					result = true;
                    break;
				}else{
					try {
						if(it.hasNextCode()){
							it.seekToCurrent();
							result = true;
		                    break;
						}
						
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}		
		return result;
	}


	public boolean hasPrev() {
		boolean result = false;

		if(itSet != null) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {
				if(it.hasPrev()) {
					result = true;
                    break;
				}else{
					try {
						if(it.hasPrevCode()){
							it.seekToCurrent();
							result = true;
		                    break;
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}		
		return result;
	}
	
	
	public Entry<InternalKey, byte[]> next() throws IOException {
		if (direction != Direction.forward) {
			for (IFileIterator<InternalKey, byte[]> it : itSet) {

				if (it != curIt) {
					try {

						it.seek(curEntry.getKey().getCode(),curEntry.getKey().getTime());
						
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			direction = Direction.forward;
		}
		curEntry = curIt.next();
		findSmallest();
		return curEntry;
	}


	public Entry<InternalKey, byte[]> prev() throws IOException {
		if(direction != Direction.reverse){
			for(IFileIterator<InternalKey, byte[]> it:itSet){
				if(curIt != it){
					try {

						it.seek(curEntry.getKey().getCode(),curEntry.getKey().getTime());
						
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			direction = Direction.reverse;
		}
		curEntry = curIt.prev();
		findLargest();
		return curEntry;		
	}


	public void seek(String table, String column, long time) throws IOException {
		
		seekKey = new InternalKey(fileManager.getCode(table),fileManager.getCode(column),time);
		
		if(null != itSet){
			for(IFileIterator<InternalKey, byte[]> it:itSet){
				it.seek(seekKey.getCode(),curSeekTime);
			}		
			findSmallest();
			direction = Direction.forward;
		}
	}
	
	public void seekToFirst() throws IOException {
		
		if(null != itSet){
			for(IFileIterator<InternalKey, byte[]> it:itSet){
				if(it.hasNextCode()){
					CodeItem item = it.nextCode();
					if(item != null){
						it.seekToCurrent();
					}
				}
			}		
			findSmallest();
			direction = Direction.forward;
		}
	}
	
	private void findSmallest() throws IOException{
		if(null != itSet){
			IFileIterator<InternalKey, byte[]> smallest = null;
			for(IFileIterator<InternalKey, byte[]> it:itSet){
				if(it.valid()){
					
					if(smallest == null){
						smallest = it;
					}else if(fileManager.compare(smallest.key(), it.key())>0){
						smallest = it;
					}else if(fileManager.compare(smallest.key(), it.key())==0){
						while(it.hasNext()){
							it.next();
							int diff = fileManager.compare(smallest.key(),it.key());
							if(0==diff){
								continue;
							}else{
								break;
							}
						}
					}
				}else{
					if(it.hasNextCode()){
						CodeItem item = it.nextCode();
						if(item != null){
							it.seekToCurrent();
						}
					}
					if(it.valid()){
						if(smallest == null){
							smallest = it;
						}else if(fileManager.compare(smallest.key(), it.key())>0){
							smallest = it;
						}else if(fileManager.compare(smallest.key(), it.key())==0){
							while(it.hasNext()){
								it.next();
								int diff = fileManager.compare(smallest.key(),it.key());
								if(0==diff){
									continue;
								}else{
									break;
								}
							}
						}
					}
				}
			}
			if(smallest != null){
				curIt = smallest;
			}
		}
	}
	
	private void findLargest() throws IOException{
		if(null != itSet){
			IFileIterator<InternalKey, byte[]> largest = null;
			for(IFileIterator<InternalKey, byte[]> it:itSet){
				if(it.valid()){
					if(largest == null){
						largest = it;
					}else if(fileManager.compare(largest.key(), it.key())<0){
						largest = it;
					}else if(fileManager.compare(largest.key(), it.key())==0){
						while(it.hasPrev()){
							it.prev();
							int diff = fileManager.compare(largest.key(),it.key());
							if(0==diff){
								continue;
							}else{
								break;
							}
						}
					}
				}else{
					if(it.hasPrevCode()){
						CodeItem item = it.prevCode();
						if(item != null){
							it.seekToCurrent();
						}
					}
					if(it.valid()){
						if(largest == null){
							largest = it;
						}else if(fileManager.compare(largest.key(), it.key())<0){
							largest = it;
						}else if(fileManager.compare(largest.key(), it.key())==0){
							while(it.hasPrev()){
								it.prev();
								int diff = fileManager.compare(largest.key(),it.key());
								if(0==diff){
									continue;
								}else{
									break;
								}
							}
						}
					}
				}
			}
			if(largest != null){
				curIt = largest;
			}
		}
	}
	

	public String table() {
		if(curEntry != null){
			return fileManager.getName(curEntry.getKey().getTableCode());
		}
		return null;
	}

	
	public String column() {
		if(curEntry != null){
			return fileManager.getName(curEntry.getKey().getColumnCode());
		}
		return null;
	}


	public long time() {
		if(curEntry != null){
			return curEntry.getKey().getTime();
		}
		return 0;
	}


	public byte[] value() throws IOException {
		if(curEntry != null){
			return curEntry.getValue();
		}
		return null;
	}


	public boolean valid() {
		if(curEntry==null){
			return false;
		}else{
			return true;
		}
	}


	public void close() throws IOException{
		
		if(null != itSet){
			for(IFileIterator<InternalKey, byte[]> it:itSet){
				it.close();
			}
		}
	}


	public InternalKey key() {
		if(curEntry != null){
			return curEntry.getKey();
		}
		return null;
	}
	
	
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}
	
	enum Direction{
		forward,reverse
	}

}
