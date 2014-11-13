package com.ctriposs.tsdb.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Random;

import com.ctriposs.tsdb.DBConfig;
import com.ctriposs.tsdb.DBEngine;
import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;
import com.ctriposs.tsdb.test.util.TestUtil;

public class DBEngineSeekFunctionTest {

    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR +"/seek_test";
    private static final int INIT_COUNT = 10*1000*1000;
    private static DBEngine engine;

    public static void main(String[] args) throws IOException {

    	int numKeyLimit = 300;
        DBConfig config = new DBConfig(TEST_DIR);
        engine = new DBEngine(config);


        Random random = new Random();
        long start = System.nanoTime();

        String data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();


        String startKey = null;
        long startTime = 0;
        long lastTime = 0;
        String lastValue = null;
        
        for (int i = 0; i < 2*INIT_COUNT; i++) {
        	String rndKey = String.valueOf(random.nextInt(numKeyLimit));
            long time = System.currentTimeMillis();
            String key = rndKey+"-"+time;
            String value = data+i;
            if(i==0){
            	startKey = rndKey;
            	startTime = time;
            }
        	
        	engine.put(rndKey, rndKey, time, value.getBytes());
        	    
            if(rndKey.equals(startKey)){
            	map.put(key,value);
            	lastTime= time;
            	lastValue = value;
            }
        }
        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second single thread%n",
                (int) (INIT_COUNT * 2 * 1e6 / duration));

        System.out.println("start value:"+new String(engine.get(startKey, startKey, startTime)));

        ISeekIterator<InternalKey, byte[]> eIt = engine.iterator();

        eIt.seek(startKey, startKey, startTime);
        int count = 0;
        Iterator<Entry<String,String>> mIt = map.entrySet().iterator();
        
        while(mIt.hasNext()){
        	eIt.next();
        	Entry<String,String> mEntry = mIt.next();
        	if(eIt.value()!=null){
	        	if(!mEntry.getValue().equals(new String(eIt.value()))){
	        		System.out.println(++count+":"+eIt.time()+":"+eIt.table()+":"+new String(eIt.value()));
	        	}else{
	        		++count;
	        	}
        	}else{
        		System.out.println(mEntry.getValue());
        	}
        }
        
        
        System.out.println("map size:"+map.size());
        System.out.println("last enging value:"+new String(engine.get(startKey, startKey, lastTime)));
        System.out.println("last value:"+lastValue);
        


    }
}
