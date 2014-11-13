package com.ctriposs.tsdb.unit;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IStorage;
import com.ctriposs.tsdb.common.PureFileStorage;
import com.ctriposs.tsdb.iterator.FileSeekIterator;
import com.ctriposs.tsdb.storage.CodeItem;
import com.ctriposs.tsdb.storage.DBWriter;
import com.ctriposs.tsdb.test.util.TestUtil;
import com.ctriposs.tsdb.util.FileUtil;

public class FileSeekIteratorUnitTest {

	private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "unit/fileseekiterator/";

	private static FileSeekIterator fIterator;
	private static String fileName;
	private static long timecount = 300000;
	private static long maxtime = 200;
	private static long start = 0;
	private static Random random = new Random();

	@Before
	public void setup() throws IOException {

		IStorage storage = new PureFileStorage(TEST_DIR,System.currentTimeMillis(), "dat");
		fileName = storage.getName();
		DBWriter dbWriter = new DBWriter(storage, timecount, 1);
		start = System.currentTimeMillis();
		int code = 0;
		int value = 0;

		for (int i = 0,count = 0; i < timecount; i++,count++) {
			
			if(count==maxtime){
				count = 0;
				code++;
				value = 0;
			}
			InternalKey key = new InternalKey(code, start++);
			dbWriter.add(key, (String.valueOf(value++)).getBytes());
		}
		
		dbWriter.close();
		fIterator = new FileSeekIterator(new PureFileStorage(new File(fileName)));
	}

	@Test
	public void testOneCodeIteratorNext() throws IOException {

		int maxCode = (int) (timecount/maxtime);
		//fIterator.seekToFirst(random.nextInt(maxCode));
		fIterator.seek(random.nextInt(maxCode),start);
		int count = 0;
		while (fIterator.hasNext()) {
			Entry<InternalKey, byte[]> entry = fIterator.next();
			String value = new String(entry.getValue());
			Assert.assertEquals(String.valueOf(count++), value);
		}
	}

	@Test
	public void testAllCodeIteratorNext() throws IOException {
		int code = 0;
		int count = 0;
		while(fIterator.hasNextCode()){
			CodeItem codeItem = fIterator.nextCode();
			Assert.assertEquals(code++, codeItem.getCode());
			fIterator.seekToCurrent(true);
			int index = 0;
			while (fIterator.hasNext()) {
				Entry<InternalKey, byte[]> entry = fIterator.next();
				String value = new String(entry.getValue());
				Assert.assertEquals(String.valueOf(index++), value);
				count++;
			}
		}

		Assert.assertEquals(timecount, count);		

	}

	@Test
	public void testOneCodeIteratorPrev() throws IOException {

		int maxCode = (int) (timecount/maxtime);
		fIterator.seekToFirst(random.nextInt(maxCode));
		int count = 0;
		while (fIterator.hasNext()) {
			Entry<InternalKey, byte[]> entry = fIterator.next();
			String value = new String(entry.getValue());
			Assert.assertEquals(String.valueOf(count++), value);
		}
		count--;
		while (fIterator.hasPrev()) {
			Entry<InternalKey, byte[]> entry = fIterator.prev();
			String value = new String(entry.getValue());
			Assert.assertEquals(String.valueOf(count--), value);
		}
		
	}

	@Test
	public void testAllCodeIteratorPrev() throws IOException {
		int code = 0;
		int count = 0;
		while(fIterator.hasNextCode()){
			CodeItem codeItem = fIterator.nextCode();
			Assert.assertEquals(code++, codeItem.getCode());
			fIterator.seekToCurrent(true);
			int index = 0;
			while (fIterator.hasNext()) {
				Entry<InternalKey, byte[]> entry = fIterator.next();
				String value = new String(entry.getValue());
				Assert.assertEquals(String.valueOf(index++), value);
				count++;
			}
		}
		
		Assert.assertEquals(timecount, count);		
		count = 0;
		while(fIterator.hasPrevCode()){
			CodeItem codeItem = fIterator.prevCode();
			Assert.assertEquals(--code, codeItem.getCode());
			fIterator.seekToCurrent(false);
			int index = 0;
			if(count==0){
				index = (int) (timecount-code*maxtime);
			}else{
				index = (int) maxtime;
			}
			while (fIterator.hasPrev()) {
				Entry<InternalKey, byte[]> entry = fIterator.prev();
				String value = new String(entry.getValue());
				Assert.assertEquals(String.valueOf(--index), value);
				count++;
			}
		}

		Assert.assertEquals(timecount, count);		

	}
	

	@After
	public void close() throws IOException {
		fIterator.close();
		FileUtil.forceDelete(new File(fileName));
	}

}
