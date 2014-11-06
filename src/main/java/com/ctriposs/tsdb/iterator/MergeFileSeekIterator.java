package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IFileIterator;
import com.ctriposs.tsdb.manage.FileManager;

public class MergeFileSeekIterator implements ISeekIterator<InternalKey, byte[]>{
	
	private FileManager fileManager;
	private Queue<IFileIterator<InternalKey, byte[]>> itQueue;
	private Direction direction;
	private Entry<InternalKey, byte[]> curEntry;
	private IFileIterator<InternalKey, byte[]> curIt;
	private long curSeekTime;
	private InternalKey seekKey;
	
	public MergeFileSeekIterator(FileManager fileManager, IFileIterator<InternalKey, byte[]>... its) {
		this.fileManager = fileManager;
		this.itQueue = new PriorityQueue<IFileIterator<InternalKey,byte[]>>(5,new Comparator<IFileIterator<InternalKey,byte[]>>() {

			@Override
			public int compare(IFileIterator<InternalKey, byte[]> o1,
					IFileIterator<InternalKey, byte[]> o2) {
				int diff = (int) (o1.priority() - o2.priority());
				return diff;
			}
		});
		
		addIterator(its);
		this.direction = Direction.forward;
		this.curEntry = null;
		this.curIt = null;
		this.itQueue = null;
	}
	
	public void addIterator(IFileIterator<InternalKey, byte[]>... its) {
		for(IFileIterator<InternalKey, byte[]> it:its){
			itQueue.add(it);
		}
	}

	@Override
	public boolean hasNext() {

		boolean result = false;

		if(itQueue != null) {
			for (IFileIterator<InternalKey, byte[]> it : itQueue) {
				if(it.hasNext()) {
					result = true;
                    break;
				}
			}
		}		
		return result;
	}


	@Override
	public boolean hasPrev() {
		boolean result = false;

		if(itQueue != null) {
			for (IFileIterator<InternalKey, byte[]> it : itQueue) {
				if(it.hasPrev()) {
					result = true;
                    break;
				}
			}
		}		
		return result;
	}
	
	@Override
	public Entry<InternalKey, byte[]> next() {
		if (direction != Direction.forward) {
			for (IFileIterator<InternalKey, byte[]> it : itQueue) {

				if (it != curIt) {
					try {
						if (it.hasNext()) {
							it.seek(seekKey.getCode(),curSeekTime);
						}
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

	@Override
	public Entry<InternalKey, byte[]> prev() {
		if(direction != Direction.reverse){
			for(IFileIterator<InternalKey, byte[]> it:itQueue){
				if(curIt != it){
					try {
						if(it.hasNext()){
							it.seek(seekKey.getCode(),curSeekTime);
						}
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

	@Override
	public void seek(String table, String column, long time) throws IOException {
		
		seekKey = new InternalKey(fileManager.getCode(table),fileManager.getCode(column),time);
		
		if(null != itQueue){
			for(IFileIterator<InternalKey, byte[]> it:itQueue){
				it.seek(seekKey.getCode(),curSeekTime);
			}		
			findSmallest();
			direction = Direction.forward;
		}
	}
	
	private void findSmallest(){
		if(null != itQueue){
			IFileIterator<InternalKey, byte[]> smallest = null;
			for(IFileIterator<InternalKey, byte[]> it:itQueue){
				itQueue.remove(it);
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
					itQueue.add(it);
				}
			}
			curIt = smallest;
		}
	}
	
	private void findLargest(){
		if(null != itQueue){
			IFileIterator<InternalKey, byte[]> largest = null;
			for(IFileIterator<InternalKey, byte[]> it:itQueue){
				itQueue.remove(it);
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
					itQueue.add(it);
				}
			}
			curIt = largest;
		}
	}
	
	@Override
	public String table() {
		if(curEntry != null){
			fileManager.getName(curEntry.getKey().getTableCode());
		}
		return null;
	}

	@Override
	public String column() {
		if(curEntry != null){
			fileManager.getName(curEntry.getKey().getColumnCode());
		}
		return null;
	}

	@Override
	public long time() {
		if(curEntry != null){
			return curEntry.getKey().getTime();
		}
		return 0;
	}

	@Override
	public byte[] value() throws IOException {
		if(curEntry != null){
			return curEntry.getValue();
		}
		return null;
	}

	@Override
	public boolean valid() {
		if(curEntry==null){
			return false;
		}else{
			return true;
		}
	}

	@Override
	public void close() throws IOException{
		
		if(null != itQueue){
			for(IFileIterator<InternalKey, byte[]> it:itQueue){
				it.close();
			}
		}
	}

	@Override
	public InternalKey key() {
		if(curEntry != null){
			return curEntry.getKey();
		}
		return null;
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}
	
	enum Direction{
		forward,reverse
	}

}
