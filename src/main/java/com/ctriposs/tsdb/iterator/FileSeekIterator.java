package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Map.Entry;

import com.ctriposs.tsdb.DBConfig;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.common.IStorage;
import com.ctriposs.tsdb.storage.CodeBlock;
import com.ctriposs.tsdb.storage.CodeItem;
import com.ctriposs.tsdb.storage.Head;
import com.ctriposs.tsdb.storage.TimeBlock;
import com.ctriposs.tsdb.storage.TimeItem;

public class FileSeekIterator implements IFileIterator<InternalKey, byte[]> {

	private IStorage storage;
	private int maxCodeBlockIndex = 0;
	private int maxTimeBlockIndex = 0;
	private int curCodeBlockIndex = -1;
	private int curTimeBlockIndex = -1;
	
	private Entry<InternalKey, byte[]> curEntry;
	private TimeBlock curTimeBlock;
	private CodeBlock curCodeBlock;
	private CodeItem curCodeItem;
	private Head head;
	
	public FileSeekIterator(IStorage storage)throws IOException {
		this.storage = storage;
		byte[] bytes = new byte[Head.HEAD_SIZE];
		this.storage.get(0, bytes);
		this.head = new Head(bytes);
		this.maxCodeBlockIndex = (head.getCodeCount()+DBConfig.BLOCK_MAX_COUNT)/DBConfig.BLOCK_MAX_COUNT - 1;
		this.curEntry = null;
		this.curTimeBlock = null;
		this.curCodeBlock = null;
		this.curCodeItem = null;
	}


	@Override
	public boolean hasNext() {
		
		return false;
	}



	@Override
	public Entry<InternalKey, byte[]> next() {


		return null;
	}

	private void nextCodeBlock() throws IOException{
		curCodeBlockIndex++;
		byte[] bytes = null;
		int count = 0;
		if(curCodeBlockIndex == maxCodeBlockIndex){
			count = head.getCodeCount() - curCodeBlockIndex*DBConfig.BLOCK_MAX_COUNT;
		}else{
			count = DBConfig.BLOCK_MAX_COUNT;		
		}
		bytes = new byte[count*CodeItem.CODE_ITEM_SIZE];
		storage.get(head.getCodeOffset()+curCodeBlockIndex*DBConfig.BLOCK_MAX_COUNT*CodeItem.CODE_ITEM_SIZE, bytes);
		curCodeBlock = new CodeBlock(bytes, count);

	}
	
	private void nextTimeBlock() throws IOException{
		
		curTimeBlockIndex++;
		byte[] bytes = null;
		int count = 0;
		if(curTimeBlockIndex == maxTimeBlockIndex){
			count = curCodeItem.getTimeCount() - curTimeBlockIndex*DBConfig.BLOCK_MAX_COUNT;
		}else{
			count = DBConfig.BLOCK_MAX_COUNT;		
		}
		bytes = new byte[count*TimeItem.TIME_ITEM_SIZE];
		storage.get(curCodeItem.getTimeOffSet()+curTimeBlockIndex*DBConfig.BLOCK_MAX_COUNT+TimeItem.TIME_ITEM_SIZE, bytes);
		curTimeBlock = new TimeBlock(bytes, count);
	}

	@Override
	public void seek(int code, long time) throws IOException {
		
		
	}


	@Override
	public void seekToFirst(int code) throws IOException {
		int index = 0;
		if (head.contain(code)) {
			while (true) {
				nextCodeBlock();

				if (curCodeBlock.seek(code)) {
					curCodeItem = curCodeBlock.current();
					maxCodeBlockIndex = (curCodeItem.getTimeCount()+DBConfig.BLOCK_MAX_COUNT)/DBConfig.BLOCK_MAX_COUNT - 1;
					curTimeBlockIndex = -1;
					break;
				}
			}
			//read time
			if(curCodeItem != null){
				readTimeItem(curCodeItem.getTimeOffSet());
			}
		}
	}
	
	private void readTimeItem(long offset){
		
	}

	@Override
	public CodeItem nextCode() throws IOException {
		
		return null;
	}



	@Override
	public CodeItem prevCode() throws IOException {


		return null;
	}



	@Override
	public InternalKey key() {


		return null;
	}



	@Override
	public long time() {


		return 0;
	}



	@Override
	public byte[] value() throws IOException {


		return null;
	}



	@Override
	public boolean valid() {


		return false;
	}



	@Override
	public Entry<InternalKey, byte[]> prev() {


		return null;
	}



	@Override
	public void close() throws IOException {

		storage.close();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}

}
