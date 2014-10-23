package com.ctriposs.leveldb.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;

import com.ctriposs.leveldb.table.Table;

public class MapFileTable extends Table{

	
	public MapFileTable(String name, FileChannel fileChannel) throws IOException {
		super(name, fileChannel);
		// TODO Auto-generated constructor stub
	}


}
