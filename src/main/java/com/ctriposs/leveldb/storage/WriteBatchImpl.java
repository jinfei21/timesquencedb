package com.ctriposs.leveldb.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import com.ctriposs.leveldb.IWriteBatch;
import com.ctriposs.leveldb.table.Slice;
import static com.google.common.collect.Lists.newArrayList;

public class WriteBatchImpl implements IWriteBatch{

	private List<Entry<Slice,Slice>> batch = newArrayList();
	private int usedSize;
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IWriteBatch put(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IWriteBatch delete(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}
