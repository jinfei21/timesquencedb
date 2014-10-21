package com.ctriposs.leveldb.table;

import java.util.Comparator;


public interface UserComparator extends Comparator<Slice>{

	String name();
	Slice findShortSeparator(Slice start,Slice limit);
	Slice findShortSuccessor(Slice key);
}
