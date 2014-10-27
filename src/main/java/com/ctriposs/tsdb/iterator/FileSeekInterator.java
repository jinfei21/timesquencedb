package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Map.Entry;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalEntry;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.NameManager;
import com.ctriposs.tsdb.storage.DataMeta;
import com.ctriposs.tsdb.util.ByteUtil;

public class FileSeekInterator implements ISeekIterator<InternalKey, byte[]>{
	
	private final NameManager nameManager;
	private int current = -1;
	private IStorage storage;
	private int count = 0;
	private DataMeta curMeta;
	private Entry<InternalKey, byte[]> curEntry;
	private InternalKey seekKey;
	
	public FileSeekInterator(IStorage storage,NameManager nameManager) throws IOException{
		this.storage = storage;
		byte[] bytes = new byte[4];
		this.storage.get(0, bytes);
		this.count = ByteUtil.ToInt(bytes);		
		this.curMeta = null;
		this.nameManager = nameManager;
	}

	@Override
	public boolean hasNext() {
		if(current < count){
			return true;
		}else{
			return false;			
		}
	}

	@Override
	public Entry<InternalKey, byte[]> next() {
		if(current < count){
			current++;
			try {
				curMeta = read(current);
				InternalKey key = new InternalKey(curMeta.getCode(),curMeta.getTime());
				byte[] value = new byte[curMeta.getValueSize()];
				storage.get(curMeta.getOffSet(), value);
				curEntry = new InternalEntry(key, value);
			} catch (IOException e) {
				e.printStackTrace();
				curEntry = null; 
			}

		}else{
			curEntry = null; 
		}
		return curEntry;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}

	@Override
	public void seek(String table, String column, long time)throws IOException {
		
		seekKey = new InternalKey(nameManager.getCode(table),nameManager.getCode(column),time);
		int left = 0;
		int right = count - 1;
		while(left < right){
			int mid = (left + right + 1)/2;
			DataMeta meta = read(mid);
			if(seekKey.getCode() < meta.getCode()){
				right = mid - 1;
			}else{
				left = mid + 1;
			}
		}
		
		for(current = left;current<count;current++){
			curMeta = read(current);
			if(curMeta.getTime()>=time){
				break;
			}
		}
	}

	private DataMeta read(int index) throws IOException{
		byte[] bytes = new byte[DataMeta.META_SIZE];
		storage.get(4+DataMeta.META_SIZE*index, bytes);
		return new DataMeta(bytes);
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
	public byte[] value() {
		if(curEntry != null){
			return curEntry.getValue();
		}
		return null;
	}

	@Override
	public boolean valid() {
		if(curEntry == null ){
			return false;
		}else{
			return true;
		}
	}

	@Override
	public Entry<InternalKey, byte[]> prev() {
		if(current > 0){
			current--;
			try {
				curMeta = read(current);
				InternalKey key = new InternalKey(curMeta.getCode(),curMeta.getTime());
				byte[] value = new byte[curMeta.getValueSize()];
				storage.get(curMeta.getOffSet(), value);
				curEntry = new InternalEntry(key, value);
			} catch (IOException e) {
				e.printStackTrace();
				curEntry = null;
			}
		}else{
			curEntry = null; 
		}
		return curEntry;
	}

	@Override
	public void close() throws IOException {
		storage.close();
	}

	@Override
	public InternalKey key() {
		if(curEntry != null){
			return curEntry.getKey();
		}
		return null;
	}
	
}
