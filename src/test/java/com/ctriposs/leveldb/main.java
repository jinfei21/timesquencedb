package com.ctriposs.leveldb;

import java.util.concurrent.locks.ReentrantLock;

public class main {

	public static void main(String[] args) {
		

	}
	
	class A{
		
		private ReentrantLock mutex;
		public A(){
			mutex = new ReentrantLock();
		}
		
		public void add(){
			try{
				mutex.lock();
				
				
				
				
			}finally{
				mutex.unlock();
			}
		}
		
	}

}
