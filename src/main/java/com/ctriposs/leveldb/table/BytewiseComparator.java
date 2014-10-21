package com.ctriposs.leveldb.table;

import com.ctriposs.leveldb.util.SliceUtil;

public class BytewiseComparator implements UserComparator{

	@Override
	public int compare(Slice o1, Slice o2) {
		return o1.compareTo(o2);
	}

	@Override
	public String name() {
		return this.getClass().getName();
	}

	@Override
	public Slice findShortSeparator(Slice start, Slice limit) {
		int shared = SliceUtil.calSharedCount(start, limit);
		if(shared < Math.min(start.length(), limit.length())){
			int lastSharedByte = start.getUnsignedByte(shared);
			if(lastSharedByte < 0xFF && lastSharedByte + 1 < limit.getUnsignedByte(shared)){
				Slice result = start.copySlice(0, lastSharedByte + 1);
				result.setByte(shared, lastSharedByte + 1);
			}
		}
		
		return start;
	}

	@Override
	public Slice findShortSuccessor(Slice key) {
		
		for(int i=0;i < key.length();i++){
			int b = key.getUnsignedByte(i);
			if(b != 0xFF){
				Slice result = key.copySlice(0,i+1);
				result.setByte(i, b + 1);
				return result;
			}
		}
		
		return key;
	}

	
}
