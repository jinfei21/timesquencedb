package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.FileManager;
import com.ctriposs.tsdb.manage.NameManager;
import com.ctriposs.tsdb.storage.FileMeta;
import com.ctriposs.tsdb.storage.PureFileStorage;
import com.ctriposs.tsdb.table.InternalKeyComparator;

public class SeekIteratorAdapter implements ISeekIterator<InternalKey, byte[]>{
	
	private FileManager fileManager;
	private NameManager nameManager;
	private InternalKeyComparator internalKeyComparator;
	private Entry<InternalKey, byte[]> currentEntry;
	private List<ISeekIterator> currentIterators;
	private Direction direction;
	
	public SeekIteratorAdapter(FileManager fileManager,NameManager nameManager,InternalKeyComparator internalKeyComparator){
		this.fileManager = fileManager;
		this.nameManager = nameManager;
		this.internalKeyComparator = internalKeyComparator;
		this.currentEntry = null;
		this.direction = Direction.forward;
	}

	@Override
	public boolean hasNext() {
		
		return false;
	}

	@Override
	public Entry<InternalKey, byte[]> next() {


		return null;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}
	
	private long format(long time){
		return time/60000*60000;
	}
	
	@Override
	public void seek(String table, String column, long time) throws IOException {
		currentIterators = getNextIterators(time);
		if(null != currentIterators){
			for(ISeekIterator<InternalKey, byte[]> it:currentIterators){
				it.seek(table, column, time);
			}
		}
		
	}
	
	private List<ISeekIterator> getNextIterators(long time) throws IOException{
		
		List<FileMeta> metas = fileManager.getFiles(format(time));
		if(metas != null){
			List<ISeekIterator> list = new ArrayList<ISeekIterator>();
			for(FileMeta meta:metas){
				list.add(new FileSeekInterator(new PureFileStorage(meta.getFile(), meta.getFile().length()), nameManager));
			}
			return list;
		}
		return null;
	}
	

	@Override
	public String table() {
		
		return null;
	}

	@Override
	public String column() {
			
		return null;
	}

	@Override
	public long time() {
		
		return 0;
	}

	@Override
	public byte[] value()throws IOException {
		
		return null;
	}

	@Override
	public boolean valid() {
		
		return false;
	}

	@Override
	public void prev() {
		
		
	}

	@Override
	public void close() throws IOException{
		
		
	}

	
	enum Direction{
		forward,reverse
	}
}
