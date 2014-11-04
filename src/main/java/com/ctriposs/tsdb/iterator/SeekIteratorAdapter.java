package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IFileIterator;
import com.ctriposs.tsdb.common.PureFileStorage;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.table.MemTable;

public class SeekIteratorAdapter implements ISeekIterator<InternalKey, byte[]>{
	
	private List<ISeekIterator<InternalKey, byte[]>> iterators;
	private Direction direction;
	private ISeekIterator<InternalKey, byte[]> curIterator;
	private FileManager fileManager;
	private long curSeekTime;
	private String curSeekTable;
	private String curSeekColumn;
	
	public SeekIteratorAdapter(FileManager fileManage,ISeekIterator<InternalKey, byte[]>... its) {

		this.iterators = new ArrayList<ISeekIterator<InternalKey,byte[]>>();
		for(ISeekIterator<InternalKey, byte[]> it:its){
			iterators.add(it);
		}
		this.fileManager = fileManager;
		this.direction = Direction.forward;
		this.curIterator = null;
		this.iterators = null;
	}
	
	public void addIterator(ISeekIterator<InternalKey, byte[]>... its) {
		for(ISeekIterator<InternalKey, byte[]> it:its){
			iterators.add(it);
		}
	}

	@Override
	public boolean hasNext() {

		boolean result = false;

		if(iterators != null) {
			for (ISeekIterator<InternalKey, byte[]> it : iterators) {
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
			for (ISeekIterator<InternalKey, byte[]> it : iterators) {

				if (it != curIterator) {
					try {
						if (it.hasNext()) {
							it.seek(curSeekTable,curSeekColumn,curSeekTime);
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			direction = Direction.forward;
		}
		Entry<InternalKey, byte[]> entry = curIterator.next();
		findSmallest();
		return entry;
	}

	@Override
	public Entry<InternalKey, byte[]> prev() {
		if(direction != Direction.reverse){
			for(ISeekIterator<InternalKey, byte[]> it:iterators){
				if(curIterator != it){
					try {
						if(it.hasNext()){
							it.seek(curSeekTable,curSeekColumn,curSeekTime);
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			direction = Direction.reverse;
		}
		Entry<InternalKey, byte[]> entry = curIterator.prev();
		findLargest();
		return entry;		
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}
	
	@Override
	public void seek(String table, String column, long time) throws IOException {
		this.curSeekTable = table;
		this.curSeekColumn = column;
		this.curSeekTime = time;
		
		if(null != iterators){
			for(ISeekIterator<InternalKey, byte[]> it:iterators){
				it.seek(table,  column,time);
			}		
			findSmallest();
			direction = Direction.forward;
		}
	}
	
	private void findSmallest(){
		if(null != iterators){
			ISeekIterator<InternalKey, byte[]> smallest = null;
			for(ISeekIterator<InternalKey, byte[]> it:iterators){
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
			ISeekIterator<InternalKey, byte[]> largest = null;
			for(ISeekIterator<InternalKey, byte[]> it:iterators){
				if(it.valid()){
					if(largest == null){
						largest = it;
					}else if(fileManager.compare(largest.key(), it.key())<0){
						largest = it;
					}
				}
			}
			curIterator = largest;
		}
	}

	@Override
	public String table() {
		if(curIterator != null){
			curIterator.table();
		}
		return null;
	}

	@Override
	public String column() {
		if(curIterator != null){
			curIterator.column();
		}
		return null;
	}

	@Override
	public long time() {
		if(curIterator != null){
			return curIterator.key().getTime();
		}
		return 0;
	}

	@Override
	public byte[] value() throws IOException {
		if(curIterator != null){
			return curIterator.value();
		}
		return null;
	}

	@Override
	public boolean valid() {
		if(curIterator==null){
			return false;
		}else{
			return true;
		}
	}

	@Override
	public void close() throws IOException{
		
		if(null != iterators){
			for(ISeekIterator<InternalKey, byte[]> it:iterators){
				it.close();
			}
		}
	}

	@Override
	public InternalKey key() {
		if(curIterator != null){
			return curIterator.key();
		}
		return null;
	}
	
	enum Direction{
		forward,reverse
	}
}
