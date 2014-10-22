package com.ctriposs.leveldb.util;

import com.ctriposs.leveldb.Constant;
import com.ctriposs.leveldb.table.Slice;

public class SliceUtil {

    public static Slice getUserKey(Slice data)
    {
        return data.copySlice(0, data.length() - Constant.SIZE_OF_LONG);
    }
    
    public static int calSharedCount(Slice left,Slice right){
    	int shared = 0;
    	if(left != null && right != null){
    		int min = Math.min(left.length(), right.length());
    		while(shared < min && left.getByte(shared)==right.getByte(shared) ){
    			shared++;
    		}
    	}
    	
    	return shared;
    }
    
    public static Slice allocate(int capacity){
        if (capacity == 0) {
            return Constant.EMPTY_SLICE;
        }
        return new Slice(capacity);
    }
}
