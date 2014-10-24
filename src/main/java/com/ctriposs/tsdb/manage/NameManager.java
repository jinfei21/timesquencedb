package com.ctriposs.tsdb.manage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NameManager {

	private Map<String,Integer> tableMap = new ConcurrentHashMap<String,Integer>();
	private Map<String,Integer> columnMap = new ConcurrentHashMap<String,Integer>();
	
	private Lock tableLock = new ReentrantLock();
	private Lock columnLock = new ReentrantLock();
	private AtomicInteger maxCode = new AtomicInteger(0);
	
	public NameManager(String dir){
		
	}
	
	public int getTableCode(String tableName){
		return getCode(tableMap,tableLock,tableName);
	}
	
	public int getColumnCode(String colName){
		return getCode(columnMap,columnLock,colName);
	}
	
	private int getCode(Map<String,Integer> map,Lock lock,String name){
		Integer code = map.get(name);
		if(code==null){
			try{
				lock.lock();
				code = map.get(name);
				if(code==null){
					code = maxCode.getAndIncrement();
					map.put(name, code);
				}
			}finally{
				lock.unlock();
			}
		}
		return code;
	}
	
	public int getCode(String tableName,String columnName){
		return getTableCode(tableName)*getColumnCode(columnName);
	}
}
