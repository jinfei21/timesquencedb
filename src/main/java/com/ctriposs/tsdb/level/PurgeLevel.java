package com.ctriposs.tsdb.level;

import com.ctriposs.tsdb.manage.FileManager;

public class PurgeLevel implements Runnable{
	public final static int MAX_PERIOD = 1000*60*60*24*30;
	
	
	private FileManager fileManager;
	private volatile boolean run = false;
	
	public PurgeLevel(FileManager fileManager){
		this.fileManager = fileManager;
	}

	public void start(){
		if(!run){
			run = true;
			new Thread(this).start();
			
		}
	}
	
	public void stop(){
		run = false;
	}

	@Override
	public void run() {

		while(run){
			
		}
		
	}

}
