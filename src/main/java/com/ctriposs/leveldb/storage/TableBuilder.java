package com.ctriposs.leveldb.storage;

import java.io.IOException;

import com.ctriposs.leveldb.EngineConfig;
import com.ctriposs.leveldb.table.Slice;
import com.google.common.base.Preconditions;

public class TableBuilder {

	
	public TableBuilder(EngineConfig config){
		Preconditions.checkNotNull(config, "Config is null!");
		
	}
	
	public void add(Slice key,Slice value) throws IOException{
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        
        
	}
}
