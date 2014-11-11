package com.ctriposs.tsdb.test;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class main {

	public static void main(String[] args) {
		
		ConcurrentSkipListMap<Integer, Integer> data = new ConcurrentSkipListMap<Integer, Integer>();
		
		data.put(1, 1);
		data.put(2, 2);
		data.put(3, 3);
		data.put(4, 4);
		data.put(5, 5);
		data.put(6, 6);
		data.put(7, 7);
		data.put(8, 8);
		data.put(11, 11);
		data.put(21, 21);
		data.put(31, 31);
		data.put(41, 41);
		System.out.println(data.lowerEntry(50));
		System.out.println("-------------");
		Iterator<Entry<Integer,Integer>> it = data.tailMap(50).entrySet().iterator();
		while(it.hasNext()){
			System.out.println(it.next());
		}
	}

}
