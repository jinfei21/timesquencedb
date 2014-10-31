package com.ctriposs.tsdb.manage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NameManager {

	private Map<String,Short> nameMap = new ConcurrentHashMap<String,Short>();
	private Map<Short,String> codeMap = new ConcurrentHashMap<Short,String>();
	private Lock lock = new ReentrantLock();
	private AtomicInteger maxCode = new AtomicInteger(1);

	
	public NameManager(String dir){
	
	}
	
	public short getCode(String name){
		Short code = nameMap.get(name);
		if(code==null){
			try{
				lock.lock();
				code = nameMap.get(name);
				if(code==null){
					code = (short) maxCode.incrementAndGet();
					nameMap.put(name, code);
					codeMap.put(code, name);
				}
			}finally{
				lock.unlock();
			}
		}
		return code;
	}
	
	public String getName(short code){
		return codeMap.get(code);
	}
}
