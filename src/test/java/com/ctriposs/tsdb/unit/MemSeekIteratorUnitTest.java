package com.ctriposs.tsdb.unit;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ctriposs.tsdb.table.InternalKeyComparator;
import com.ctriposs.tsdb.table.MemTable;
import com.ctriposs.tsdb.test.util.TestUtil;
import com.ctriposs.tsdb.util.FileUtil;

public class MemSeekIteratorUnitTest {
	private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "unit/memseekiterator/";
	private static MemTable memTable;
	private static long timecount = 300000;
	private static long maxtime = 200;
	
	
	@Before
	public void setup() throws IOException{
		memTable = new MemTable(TEST_DIR, 1, MemTable.MAX_MEM_SIZE, MemTable.MAX_MEM_SIZE, new InternalKeyComparator());

	}
	
	@Test
	public void iterator(){
		
		
		
	}
	
	
	@After
	public void close() throws IOException{
		memTable.close();
		FileUtil.forceDelete(new File(memTable.getLogFile()));
	}
}
