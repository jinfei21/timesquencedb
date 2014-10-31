package com.ctriposs.tsdb.iterator;

import java.io.IOException;
import java.util.Map.Entry;

import com.ctriposs.tsdb.IStorage;
import com.ctriposs.tsdb.InternalEntry;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.storage.IndexBlock;
import com.ctriposs.tsdb.storage.IndexHead;
import com.ctriposs.tsdb.storage.IndexMeta;

public class FileSeekIterator implements IFileIterator<InternalKey, byte[]> {

	private IStorage storage;
	private int maxBlockIndex = 0;
	private int curBlockIndex = 0;
	private Entry<InternalKey, byte[]> curEntry;
	private IndexBlock curBlock;
	private IndexHead head;
	
	public FileSeekIterator(IStorage storage)throws IOException {
		this.storage = storage;
		byte[] bytes = new byte[IndexHead.HEAD_SIZE];
		this.storage.get(0, bytes);
		this.head = new IndexHead(bytes);
		this.maxBlockIndex = (head.getCount()+IndexBlock.MAX_BLOCK_META_COUNT)/IndexBlock.MAX_BLOCK_META_COUNT - 1;
		this.curBlockIndex = -1;
		this.curEntry = null;
		this.curBlock = null;
	}

	@Override
	public boolean hasNext() {
		if (curBlockIndex <= maxBlockIndex) {
			if(curBlock != null){
				if (!curBlock.hashNext()){
					try {
						nextBlock();
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
					if(curBlock != null){
						return true;
					}else{
						return false;
					}				
				}else{
					return true;
				}
			}else{
				try {
					nextBlock();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				if(curBlock != null){
					return true;
				}else{
					return false;
				}
			}
		} else {
			return false;
		}
	}
	
	@Override
	public void seekToFirst() throws IOException {
		curBlockIndex = 0;
		byte[] bytes = null;
		int count = 0;
		if(curBlockIndex == maxBlockIndex){
			count = head.getCount() - curBlockIndex*IndexBlock.MAX_BLOCK_META_COUNT;

		}else{
			count = IndexBlock.MAX_BLOCK_META_COUNT;		
		}
		bytes = new byte[count*IndexMeta.META_SIZE];
		storage.get(IndexHead.HEAD_SIZE+curBlockIndex*IndexBlock.MAX_BLOCK_META_COUNT*IndexMeta.META_SIZE, bytes);
		curBlock = new IndexBlock(bytes, count);
		readEntry(curBlock.curMeta(),true);
	}


	@Override
	public void seek(int code) throws IOException {
		if(head.contain(code)){
			curBlockIndex = -1;
			nextBlock();
			if(curBlock != null){
				while(!curBlock.seekCode(code)){
					nextBlock();
					if(curBlock==null){
						curBlockIndex  = maxBlockIndex + 1;
						break;
					}
				}
				if(curBlock != null){
					readEntry(curBlock.curMeta(),true);
				}
			}else{
				curBlockIndex  = maxBlockIndex + 1;
			}
		}else{
			curBlockIndex  = maxBlockIndex + 1;
		}
	
	}
	
	private void nextBlock() throws IOException{
		curBlockIndex++;
		int count = 0;
		if(curBlockIndex == maxBlockIndex){
			count = head.getCount() - curBlockIndex*IndexBlock.MAX_BLOCK_META_COUNT;
		}else if(curBlockIndex < maxBlockIndex){
			count = IndexBlock.MAX_BLOCK_META_COUNT;			
		}else{
			curBlock = null;
			return;
		}
		byte[] bytes = new byte[count*IndexMeta.META_SIZE];
		storage.get(IndexHead.HEAD_SIZE+curBlockIndex*IndexBlock.MAX_BLOCK_META_COUNT*IndexMeta.META_SIZE, bytes);
		curBlock = new IndexBlock(bytes, count);
	}

	private void prevBlock() throws IOException{
		curBlockIndex--;
		int count = 0;
		if(curBlockIndex == maxBlockIndex){
			count = head.getCount() - curBlockIndex*IndexBlock.MAX_BLOCK_META_COUNT;
		}else if(curBlockIndex >= 0){
			count = IndexBlock.MAX_BLOCK_META_COUNT;
		}else{
			curBlock = null;
			return;			
		}
		byte[] bytes = new byte[count*IndexMeta.META_SIZE];
		storage.get(IndexHead.HEAD_SIZE+curBlockIndex*IndexBlock.MAX_BLOCK_META_COUNT*IndexMeta.META_SIZE, bytes);
		curBlock = new IndexBlock(bytes, count);

	}
	
	private void readEntry(IndexMeta meta, boolean isNext) throws IOException {
		if (meta != null) {

			InternalKey key = new InternalKey(meta.getCode(), meta.getTime());
			byte[] value = new byte[meta.getValueSize()];
			storage.get(meta.getValueOffSet(), value);
			curEntry = new InternalEntry(key, value);
		} else {
			if (isNext) {
				curBlockIndex = maxBlockIndex + 1;
			} else {
				curBlockIndex = -1;
			}
			curEntry = null;
			curBlock = null;
		}

	}

	@Override
	public Entry<InternalKey, byte[]> next() {
		
		if(curBlock == null){
			try {
				nextBlock();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		if(curBlock != null){
			try {
				readEntry(curBlock.nextMeta(),true);
			} catch (IOException e) {
			}
		}else{
			curEntry = null;
		}
		return curEntry;
	}

	@Override
	public long time() {
		if (curEntry != null) {
			return curEntry.getKey().getTime();
		}
		return 0;
	}

	@Override
	public byte[] value() {
		if (curEntry != null) {
			return curEntry.getValue();
		}
		return null;
	}

	@Override
	public boolean valid() {
		if (curEntry == null) {
			return false;
		} else {
			return true;
		}
	}
	

	@Override
	public Entry<InternalKey, byte[]> prev() {
		if(curBlock == null){
			try {
				prevBlock();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		if(curBlock != null){
			try {
				readEntry(curBlock.prevMeta(),false);
			} catch (IOException e) {
			}
		}else{
			curEntry = null;
		}
		return curEntry;
	}

	@Override
	public void close() throws IOException {
		storage.close();
	}

	@Override
	public InternalKey key() {
		if (curEntry != null) {
			return curEntry.getKey();
		}
		return null;
	}

	@Override
	public IndexMeta nextMeta() throws IOException {
		if (curBlock != null) {
			return curBlock.nextMeta();
		}else{
			nextBlock();
			if(curBlock != null){
				return curBlock.nextMeta();		
			}
		}
		return null;
	}

	@Override
	public IndexMeta prevMeta() throws IOException {
		if (curBlock != null) {
			return curBlock.prevMeta();
		}else{
			prevBlock();
			if(curBlock != null){
				return curBlock.prevMeta();		
			}
		}
		return null;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("unsupport remove operation!");
	}

}
