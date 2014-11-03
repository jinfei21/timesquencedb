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

public class FileSeekIterator implements IFileIterator<InternalKey, byte[]> {

	private IStorage storage;
	private int maxBlockIndex = 0;
	private int curBlockIndex = 0;
	private Entry<InternalKey, byte[]> curEntry;
	private TimeBlock curBlock;
	private Head head;
	
	public FileSeekIterator(IStorage storage)throws IOException {
		this.storage = storage;
		byte[] bytes = new byte[Head.HEAD_SIZE];
		this.storage.get(0, bytes);
		this.head = new Head(bytes);
		this.maxBlockIndex = 0;
		this.curBlockIndex = -1;
		this.curEntry = null;
		this.curBlock = null;
	}


	@Override
	public boolean hasNext() {
		
		return false;
	}



	@Override
	public Entry<InternalKey, byte[]> next() {


		return null;
	}

	private CodeBlock getNextCodeBlock(int index) throws IOException{
		
		byte[] bytes = null;
		int count = 0;
		if(curBlockIndex == maxBlockIndex){
			count = head.getCodeCount() - curBlockIndex*DBConfig.BLOCK_MAX_COUNT;

		}else{
			count = DBConfig.BLOCK_MAX_COUNT;		
		}
		bytes = new byte[count*CodeItem.CODE_ITEM_SIZE];
		storage.get(head.getCodeOffset()+0, bytes);
		CodeBlock block = new CodeBlock(bytes, count);
		return block;
	}
	
	private TimeBlock getNextTimeBlock(int index) throws IOException{
		return null;
	}

	@Override
	public void seek(int code, long time) throws IOException {
		
		
	}



	@Override
	public void seekToFirst(int code) throws IOException {
		
		
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


		
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}

}
