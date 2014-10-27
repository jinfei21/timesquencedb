package com.ctriposs.tsdb.table;

import java.util.Comparator;

import com.ctriposs.tsdb.InternalKey;



public class InternalKeyComparator implements Comparator<InternalKey>{

	@Override
	public int compare(InternalKey o1, InternalKey o2) {
		int diff = (int) (o1.getCode() - o2.getCode());
		if(diff == 0){
			diff = (int) (o1.getTime() - o2.getTime());
		}
		return diff;
	}


}
