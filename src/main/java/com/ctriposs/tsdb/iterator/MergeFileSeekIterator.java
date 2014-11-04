package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IFileIterator;
import com.ctriposs.tsdb.manage.FileManager;

public class MergeFileSeekIterator implements ISeekIterator<InternalKey, byte[]>{
	
	private FileManager fileManager;
	private List<IFileIterator<InternalKey, byte[]>> iterators;
	private Direction direction;
	private Entry<InternalKey, byte[]> curEntry;
	private IFileIterator<InternalKey, byte[]> curIterator;
	private long curSeekTime;
	private InternalKey seekKey;
	
	public MergeFileSeekIterator(FileManager fileManager, List<IFileIterator<InternalKey, byte[]>> iterators) {
		this.fileManager = fileManager;
		this.iterators = iterators;
		this.direction = Direction.forward;
		this.curEntry = null;
		this.curIterator = null;
		this.iterators = null;
	}

	@Override
	public boolean hasNext() {

		boolean result = false;

		if(iterators != null) {
			for (IFileIterator<InternalKey, byte[]> it : iterators) {
				if(it.hasNext()) {
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
			for (IFileIterator<InternalKey, byte[]> it : iterators) {

				if (it != curIterator) {
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
		curEntry = curIterator.next();
		findSmallest();
		return curEntry;
	}

	@Override
	public Entry<InternalKey, byte[]> prev() {
		if(direction != Direction.reverse){
			for(IFileIterator<InternalKey, byte[]> it:iterators){
				if(curIterator != it){
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
		curEntry = curIterator.prev();
		findLargest();
		return curEntry;		
	}

	@Override
	public void seek(String table, String column, long time) throws IOException {
		
		seekKey = new InternalKey(fileManager.getCode(table),fileManager.getCode(column),time);
		
		if(null != iterators){
			for(IFileIterator<InternalKey, byte[]> it:iterators){
				it.seek(seekKey.getCode(),curSeekTime);
			}		
			findSmallest();
			direction = Direction.forward;
		}
	}
	
	private void findSmallest(){
		if(null != iterators){
			IFileIterator<InternalKey, byte[]> smallest = null;
			for(IFileIterator<InternalKey, byte[]> it:iterators){
				if(it.valid()){
					if(smallest == null){
						smallest = it;
					}else if(fileManager.compare(smallest.key(), it.key())>0){
						smallest = it;
					}
				}
			}
			curIterator = smallest;
		}
	}
	
	private void findLargest(){
		if(null != iterators){
			IFileIterator<InternalKey, byte[]> largest = null;
			for(IFileIterator<InternalKey, byte[]> it:iterators){
				if(it.valid()){
					if(largest == null){
						largest = it;
					}else if(fileManager.compare(largest.key(), it.key())<=0){
						largest = it;
					}
				}
			}
			curIterator = largest;
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
		
		if(null != iterators){
			for(IFileIterator<InternalKey, byte[]> it:iterators){
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
