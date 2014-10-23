package com.ctriposs.leveldb.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;

import com.ctriposs.leveldb.table.Table;

public class PureFileTable extends Table{
	

	public PureFileTable(String name, FileChannel fileChannel) throws IOException {
		super(name, fileChannel);
	}


}
