package com.ctriposs.leveldb.util;

import com.ctriposs.leveldb.Constant;
import com.ctriposs.leveldb.table.Slice;

public class SliceUtil {

    public static Slice getUserKey(Slice data)
    {
        return data.copySlice(0, data.length() - Constant.SIZE_OF_LONG);
    }
    
    
}
