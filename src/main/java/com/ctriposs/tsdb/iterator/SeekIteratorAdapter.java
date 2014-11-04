package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.Level;
import com.ctriposs.tsdb.common.PureFileStorage;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.manage.NameManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.table.MemTable;

public class SeekIteratorAdapter implements ISeekIterator<InternalKey, byte[]>{
	
	private FileManager fileManager;
	private NameManager nameManager;
	private List<IFileIterator<InternalKey, byte[]>> iterators;
	private Direction direction;
	private Entry<InternalKey, byte[]> curEntry;
	private IFileIterator<InternalKey, byte[]> curIterator;
	private long curSeekTime;
	private InternalKey seekKey;
	
	public SeekIteratorAdapter(FileManager fileManager, Level level0,Map<Integer,Level> compactLevel) {
		this.fileManager = fileManager;
		this.curEntry = null;
		this.direction = Direction.forward;
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
		
		if(!result) {
			curSeekTime += MemTable.MINUTE;
			if(curSeekTime < System.currentTimeMillis()) {
				try {
					iterators = getNextIterators(curSeekTime);
					if(null != iterators){
						for(IFileIterator<InternalKey, byte[]> it:iterators){
							it.seek(seekKey.getCode(),curSeekTime);
						}		
						findSmallest();
						direction = Direction.forward;
					}
				} catch (IOException e) {
					e.printStackTrace();
					result = false;
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
			for (IFileIterator<InternalKey, byte[]> it : iterators) {

				if (it != curIterator) {
					try {
						if (it.hasNext()) {
							it.seek(seekKey.getCode(),curSeekTime);
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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
						// TODO Auto-generated catch block
						e.printStackTrace();
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
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}
	
	private long format(long time){
		return time/MemTable.MINUTE*MemTable.MINUTE;
	}
	
	@Override
	public void seek(String table, String column, long time) throws IOException {
		
		seekKey = new InternalKey(nameManager.getCode(table),nameManager.getCode(column),time);
		
		iterators = getNextIterators(format(time));
		
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
					}else if(internalKeyComparator.compare(smallest.key(), it.key())>0){
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
					}else if(internalKeyComparator.compare(largest.key(), it.key())<0){
						largest = it;
					}
				}
			}
			curIterator = largest;
		}
	}
	
	private List<IFileIterator<InternalKey, byte[]>> getNextIterators(long time) throws IOException {

		if(time > System.currentTimeMillis()){
			return null;
		}

		curSeekTime = time;
		Queue<FileMeta> metaQueue = fileManager.getFiles(time);
		if(metaQueue != null) {
			List<IFileIterator<InternalKey, byte[]>> list = new ArrayList<IFileIterator<InternalKey, byte[]>>();
			for(FileMeta meta : metaQueue) {
				list.add(new FileSeekIterator(new PureFileStorage(meta.getFile(), meta.getFile().length())));
			}
			return list;
		} else {
			return getNextIterators(time + MemTable.MINUTE);
		}
	}
	

	@Override
	public String table() {
		if(curEntry != null){
			nameManager.getName(curEntry.getKey().getTableCode());
		}
		return null;
	}

	@Override
	public String column() {
		if(curEntry != null){
			nameManager.getName(curEntry.getKey().getColumnCode());
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
	
	enum Direction{
		forward,reverse
	}
}
