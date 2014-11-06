package com.ctriposs.tsdb.table;

import java.util.Comparator;

import com.ctriposs.tsdb.InternalKey;

public class InternalKeyComparator implements Comparator<InternalKey> {

	@Override
	public int compare(InternalKey o1, InternalKey o2) {
		if(o1.getCode() == o2.getCode()){
			return (int) (o1.getTime() - o2.getTime());
		}else{
			if(o1.getCode()<o2.getCode()){
				return -1;
			}else{
				return 1;
			}
		}
		 
	}
}
