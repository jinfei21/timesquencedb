package com.ctriposs.tsdb.storage;


public class IndexBlock {

	public static final int MAX_BLOCK_META_COUNT = 200;
	private final IndexMeta metas[];
	private int curPos = 0;
	private int maxPos = -1;
	
	public IndexBlock(byte[] bytes, int count){
		this.maxPos = count - 1;
		this.metas = new IndexMeta[count]; 
		for(int i=0;i<count;i++){
			metas[i] = new IndexMeta(bytes, i*IndexMeta.META_SIZE);
		}
	}

	public boolean hashNext(){
		if(curPos < maxPos){
			return true;
		}else{
			return false;
		}
	}
	
	public boolean seekCode(long code){
	
		boolean result = false;
		int left = 0;
		int right = maxPos - 1;
		while (left < right) {
			int mid = (left + right + 1) / 2;
			if (code < metas[mid].getCode()) {
				right = mid - 1;
			} else if (code > metas[mid].getCode()) {
				left = mid + 1;
			} else {
				curPos = mid;
				break;
			}
		}
		
		if (left < right) {
			int pos = curPos - 1;
			for (; pos >= 0; pos--) {
				
				if (metas[pos].getCode() != code) {
					break;
				}
			}
			curPos = pos + 1;
			result = true;
		} else {
			curPos = maxPos + 1;
		}
		return result;
	}
	
	public IndexMeta curMeta(){
		if (curPos <= maxPos||curPos >= 0) {
			return metas[curPos];
		}
		return null;
	}

	public IndexMeta nextMeta()  {
		if (curPos <= maxPos) {
			return metas[curPos++];
		}
		return null;
	}

	
	public IndexMeta prevMeta() {
		if (curPos >= 0) {
			return metas[curPos--];
		}
		return null;
	}
}
