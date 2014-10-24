package com.ctriposs.tsdb.table;

import java.util.Comparator;

import com.ctriposs.tsdb.InternalKey;



public class InternalKeyComparator implements Comparator<InternalKey>{

	@Override
	public int compare(InternalKey o1, InternalKey o2) {
		int code = o1.getTableCode() - o2.getTableCode();
		if(code == 0){
			code = o1.getColumnCode() - o2.getColumnCode();
		}
		return code;
	}


}
