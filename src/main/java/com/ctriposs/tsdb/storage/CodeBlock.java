package com.ctriposs.tsdb.storage;


public class CodeBlock {

	
	private final CodeItem codes[];
	private int curPos = 0;
	private int maxPos = -1;
	
	public CodeBlock(byte[] bytes, int count){
		this.maxPos = count - 1;
		this.codes = new CodeItem[count]; 
		for(int i=0;i<count;i++){
			codes[i] = new CodeItem(bytes, i*CodeItem.CODE_ITEM_SIZE);
		}
	}

	public boolean hashNext(){
		if(curPos < maxPos){
			return true;
		}else{
			return false;
		}
	}
	
	public boolean seek(long code){
	
		boolean result = false;
		int left = 0;
		int right = maxPos;
		while (left < right) {
			int mid = (left + right) / 2;
			if (code < codes[mid].getCode()) {
				right = mid - 1;
			} else if (code > codes[mid].getCode()) {
				left = mid + 1;
			} else {
				curPos = mid;
				break;
			}
		}
		
		if (left < right) {
			int pos = curPos - 1;
			for (; pos >= 0; pos--) {
				
				if (codes[pos].getCode() != code) {
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
	
	public CodeItem current(){
		if (curPos <= maxPos||curPos >= 0) {
			return codes[curPos];
		}
		return null;
	}

	public CodeItem next()  {
		if (curPos <= maxPos) {
			return codes[curPos++];
		}
		return null;
	}

	
	public CodeItem prev() {
		if (curPos >= 0) {
			return codes[curPos--];
		}
		return null;
	}
}
