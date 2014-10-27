package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Map.Entry;

import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.manage.NameManager;
import com.ctriposs.tsdb.storage.DataMeta;
import com.ctriposs.tsdb.util.ByteUtil;

public class FileSeekInterator implements ISeekIterator<InternalKey, byte[]>{
	
	private final NameManager nameManager;
	private int current = 0;
	private IStorage storage;
	private int count = 0;
	private DataMeta curMeta;
	
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}

	@Override
	public void seek(String table, String column, long time) {
		InternalKey key = new InternalKey(nameManager.getCode(table),nameManager.getCode(column),time);
		
	}

	@Override
	public String table() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String column() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long time() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] value() {
		return null;
	}

	@Override
	public boolean valid() {
		return false;
	}

	@Override
	public void prev() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		storage.close();
	}

	
}
