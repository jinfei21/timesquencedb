package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.FileManager;

public class SeekIteratorAdapter implements ISeekIterator<InternalKey, byte[]>{
	
	private Queue<LevelSeekIterator> itQueue;
	private Direction direction;
	private ISeekIterator<InternalKey, byte[]> curIt;
	private FileManager fileManager;
	private long curSeekTime;
	private String curSeekTable;
	private String curSeekColumn;
	
	public SeekIteratorAdapter(FileManager fileManager, LevelSeekIterator... its) {

		this.itQueue = new PriorityQueue<LevelSeekIterator>(5, new Comparator<LevelSeekIterator>() {

			@Override
			public int compare(LevelSeekIterator o1,LevelSeekIterator o2) {
				int diff = o1.getLevelNum() - o2.getLevelNum();
				return diff;
			}
		});
		
		addIterator(its);
		this.fileManager = fileManager;
		this.direction = Direction.forward;
		this.curIt = null;
		this.itQueue = null;
	}
	
	public void addIterator(LevelSeekIterator... its) {
		for(LevelSeekIterator it:its){
			itQueue.add(it);
		}
	}

	@Override
	public boolean hasNext() {

		boolean result = false;
		if(itQueue != null) {
			for (LevelSeekIterator it : itQueue) {
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
			for (LevelSeekIterator it : itQueue) {
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
			for (LevelSeekIterator it : itQueue) {

				if (it != curIt) {
					try {
						if (it.hasNext()) {
							it.seek(curSeekTable,curSeekColumn,curSeekTime);
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			direction = Direction.forward;
		}
		Entry<InternalKey, byte[]> entry = curIt.next();
		findSmallest();
		if(entry != null){
			curSeekTime = entry.getKey().getTime();
		}
		return entry;
	}

	@Override
	public Entry<InternalKey, byte[]> prev() {
		if(direction != Direction.reverse){
			for(LevelSeekIterator it:itQueue){
				if(curIt != it){
					try {
						if(it.hasNext()){
							it.seek(curSeekTable,curSeekColumn,curSeekTime);
						}
					} catch (IOException e) {			
						throw new RuntimeException(e);
					}
				}
			}
			direction = Direction.reverse;
		}
		Entry<InternalKey, byte[]> entry = curIt.prev();
		findLargest();
		if(entry != null){
			curSeekTime = entry.getKey().getTime();
		}
		return entry;		
	}

	
	@Override
	public void seek(String table, String column, long time) throws IOException {
		this.curSeekTable = table;
		this.curSeekColumn = column;
		this.curSeekTime = time;
		
		if(null != itQueue){
			for(LevelSeekIterator it:itQueue){
				it.seek(table, column,time);
			}		
			findSmallest();
			direction = Direction.forward;
		}
	}
	
	private void findSmallest(){
		if(null != itQueue){
			LevelSeekIterator smallest = null;
			for(LevelSeekIterator it:itQueue){
				if(it.valid()){
					itQueue.remove(it);
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
						smallest = it;
					}
					itQueue.add(it);
				}
			}
			curIt = smallest;
		}
	}
	
	private void findLargest(){
		if(null != itQueue){
			LevelSeekIterator largest = null;
			for(LevelSeekIterator it:itQueue){
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
		if(curIt != null){
			curIt.table();
		}
		return null;
	}

	@Override
	public String column() {
		if(curIt != null){
			curIt.column();
		}
		return null;
	}

	@Override
	public long time() {
		if(curIt != null){
			return curIt.key().getTime();
		}
		return 0;
	}

	@Override
	public byte[] value() throws IOException {
		if(curIt != null){
			return curIt.value();
		}
		return null;
	}

	@Override
	public boolean valid() {
		if(curIt==null){
			return false;
		}else{
			return curIt.valid();
		}
	}

	@Override
	public void close() throws IOException{
		
		if(null != itQueue){
			for(LevelSeekIterator it:itQueue){
				it.close();
			}
		}
	}

	@Override
	public InternalKey key() {
		if(curIt != null){
			return curIt.key();
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
