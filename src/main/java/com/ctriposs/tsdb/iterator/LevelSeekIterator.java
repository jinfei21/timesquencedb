package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IFileIterator;
import com.ctriposs.tsdb.common.Level;
import com.ctriposs.tsdb.common.PureFileStorage;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;

public class LevelSeekIterator implements ISeekIterator<InternalKey, byte[]>{
	
	private FileManager fileManager;
	private Queue<IFileIterator<InternalKey, byte[]>> itQueue;
	private Direction direction;
	private Entry<InternalKey, byte[]> curEntry;
	private IFileIterator<InternalKey, byte[]> curIt;
	private long curSeekTime;
	private InternalKey seekKey;
	private Level level;
	private long interval;
	
	public LevelSeekIterator(FileManager fileManager, Level level,long interval) {
		this.fileManager = fileManager;
		this.level = level;
		this.interval = interval;
		this.direction = Direction.forward;
		this.curEntry = null;
		this.curIt = null;
		this.itQueue = null;
		this.curSeekTime = 0;
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
		
		if(!result) {
			curSeekTime += interval;
			if(curSeekTime < System.currentTimeMillis()) {
				try {
					itQueue = getNextIterators(curSeekTime);
					if(null != itQueue){
						for(IFileIterator<InternalKey, byte[]> it:itQueue){
							it.seek(seekKey.getCode(),curSeekTime);
						}		
						findSmallest();
						direction = Direction.forward;
					}
				} catch (IOException e) {
					result = false;
					throw new RuntimeException(e);
				}
			}else{
				result = false;
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
		
		if(!result) {
			curSeekTime -= interval;
			if(curSeekTime < System.currentTimeMillis()-fileManager.getMaxPeriod()) {
				try {
					itQueue = getPrevIterators(curSeekTime);
					if(null != itQueue){
						for(IFileIterator<InternalKey, byte[]> it:itQueue){
							it.seek(seekKey.getCode(),curSeekTime);
						}		
						findLargest();
						direction = Direction.reverse;
					}
				} catch (IOException e) {
					result = false;
					throw new RuntimeException(e);
				}
			}else{
				result = false;
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
		
		itQueue = getNextIterators(level.format(time));
		
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
					}else if(fileManager.compare(largest.key(), it.key())<=0){
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
	
	private Queue<IFileIterator<InternalKey, byte[]>> getNextIterators(long time) throws IOException {

		if(time > System.currentTimeMillis()){
			return null;
		}

		curSeekTime = time;
		Queue<FileMeta> metaQueue = level.getFiles(time);
		if(metaQueue != null) {
			Queue<IFileIterator<InternalKey, byte[]>> queue = new PriorityQueue<IFileIterator<InternalKey, byte[]>>(5, new Comparator<IFileIterator<InternalKey, byte[]>>() {

				@Override
				public int compare(IFileIterator<InternalKey, byte[]> o1,IFileIterator<InternalKey, byte[]> o2) {
					int diff = (int) (o2.priority() - o1.priority());
					return diff;
				}
			});
			
			for(FileMeta meta : metaQueue) {
				queue.add(new FileSeekIterator(new PureFileStorage(meta.getFile()),meta.getFileNumber()));
			}
			
			if(itQueue!=null){
				for(IFileIterator<InternalKey, byte[]> it:itQueue){
					it.close();
				}
			}
			return queue;
		} else {
			return getNextIterators(time + interval);
		}
	}
	
	private Queue<IFileIterator<InternalKey, byte[]>> getPrevIterators(long time) throws IOException {

		if(time < System.currentTimeMillis()-fileManager.getMaxPeriod()){
			return null;
		}

		curSeekTime = time;
		Queue<FileMeta> metaQueue = level.getFiles(time);
		if(metaQueue != null) {
			Queue<IFileIterator<InternalKey, byte[]>> queue = new PriorityQueue<IFileIterator<InternalKey, byte[]>>(5, new Comparator<IFileIterator<InternalKey, byte[]>>() {

				@Override
				public int compare(IFileIterator<InternalKey, byte[]> o1,IFileIterator<InternalKey, byte[]> o2) {
					int diff = (int) (o2.priority() - o1.priority());
					return diff;
				}
			});
			
			for(FileMeta meta : metaQueue) {
				queue.add(new FileSeekIterator(new PureFileStorage(meta.getFile()),meta.getFileNumber()));
			}
			
			if(itQueue!=null){
				for(IFileIterator<InternalKey, byte[]> it:itQueue){
					it.close();
				}
			}
			return queue;
		} else {
			return getPrevIterators(time - interval);
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
	
	public int getLevelNum(){
		return level.getLevelNum();
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}
	
	enum Direction{
		forward,reverse
	}

}
