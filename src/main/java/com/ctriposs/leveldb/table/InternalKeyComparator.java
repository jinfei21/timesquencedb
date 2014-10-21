package com.ctriposs.leveldb.table;

import java.util.Comparator;
import com.google.common.primitives.Longs;

public class InternalKeyComparator implements Comparator<InternalKey>{

	private final UserComparator userComparator;
	
	public InternalKeyComparator(UserComparator userComparator) {
		this.userComparator = userComparator;	
	}
	
	public UserComparator getUserComparator(){
		return userComparator;
	}
	
	public String name(){
		return userComparator.name();
	}
	@Override
	public int compare(InternalKey o1, InternalKey o2) {
		int result = userComparator.compare(o1.getUserKey(), o2.getUserKey());
		if(result != 0){
			return result;
		}else{
			return Longs.compare(o1.getSequence(), o2.getSequence());
		}		
	}
	
	
}
