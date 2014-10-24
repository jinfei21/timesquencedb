package com.ctriposs.tsdb.level;

import java.util.concurrent.locks.Condition;

import com.ctriposs.tsdb.manage.FileManager;

public class PurgeLevel {
	public final static int MAX_SIZE = 2;
	private FileManager fileManager;
	private Condition condition;
	public PurgeLevel(FileManager fileManager,int threads){
		this.fileManager = fileManager;
		this.condition = condition;
	}

	public void start(){
		
	}
	
	public void stop(){
		
	}
	
	public void awaitUninterruptibly(){
		condition.awaitUninterruptibly();
	}
	
	public void signalAll(){
		condition.signalAll();
	}
}
