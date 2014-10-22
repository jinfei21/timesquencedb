package com.ctriposs.leveldb.storage;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.leveldb.Constant;
import com.ctriposs.leveldb.IStorage;
import com.ctriposs.leveldb.table.InternalKeyComparator;
import com.ctriposs.leveldb.table.Slice;
import com.ctriposs.leveldb.util.ByteUtil;
import com.google.common.base.Preconditions;

public class TableBuilder {

	private IStorage underlyingStorage;	
	private AtomicInteger offset = new AtomicInteger(4);
	
	public TableBuilder(File file) throws IOException{
		Preconditions.checkNotNull(file, "File is null!");
		this.underlyingStorage = new MapFileStorage(file, Constant.LEVEL0_DEFAULT_CAPACITY);

	}
	
	public void add(Slice key,Slice value) throws IOException{
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");

        int length = Constant.ITEM_META_SIZE + key.getData().length+value.getData().length;
        underlyingStorage.put(0, ByteUtil.toBytes(offset.get()));
        underlyingStorage.put(offset.get(), ByteUtil.toBytes(key.getData().length));
        underlyingStorage.put(offset.get()+4, ByteUtil.toBytes(value.getData().length));
        underlyingStorage.put(offset.get()+Constant.ITEM_META_SIZE, key.getData());
        underlyingStorage.put(offset.get()+Constant.ITEM_META_SIZE + key.getData().length, value.getData());
        offset.addAndGet(length);
	}
	
	public void finish() throws IOException{
		underlyingStorage.close();
	}
}
