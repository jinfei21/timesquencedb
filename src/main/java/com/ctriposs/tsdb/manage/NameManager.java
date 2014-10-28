package com.ctriposs.tsdb.manage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NameManager {

	private Map<String,Integer> nameMap = new ConcurrentHashMap<String,Integer>();
	private Map<Integer,String> codeMap = new ConcurrentHashMap<Integer,String>();
	private Lock lock = new ReentrantLock();
	private AtomicInteger maxCode = new AtomicInteger(0);

	
	public NameManager(String dir){
	
	}

	
	public int getCode(String name){
		Integer code = nameMap.get(name);
		if(code==null){
			try{
				lock.lock();
				code = nameMap.get(name);
				if(code==null){
					code = maxCode.incrementAndGet();
					nameMap.put(name, code);
					codeMap.put(code, name);
				}
			}finally{
				lock.unlock();
			}
		}
		return code;
	}
	
	public String getName(int code){
		return codeMap.get(code);
	}
}
