package com.ctriposs.tsdb.test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import com.ctriposs.tsdb.DBConfig;
import com.ctriposs.tsdb.DBEngine;
import com.ctriposs.tsdb.ISeekIterator;
import com.ctriposs.tsdb.InternalKey;

public class DBEngineSeekFunctionTest {

    private static final String TEST_DIR = "d:\\tsdb_test\\seek_test";
    private static final int INIT_COUNT = 10*1000*1000;
    private static DBEngine engine;

    public static void main(String[] args) throws IOException {

    	int numKeyLimit = 30000;
        DBConfig config = new DBConfig(TEST_DIR);
        engine = new DBEngine(config);


        Random random = new Random();
        long start = System.nanoTime();

        String data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Map<String,String> map = new LinkedHashMap<String,String>();

        long startTime = 0;
        long lastTime = 0;
        String startKey = null;
        String startValue = null;
        String lastValue = null;
        
        for (int i = 0; i < 2*INIT_COUNT; i++) {
        	String rndKey = String.valueOf(random.nextInt(numKeyLimit));
            long time = System.currentTimeMillis();
            String key = rndKey+"-"+time;
            String value = data+i;
            if(i==0){
            	startTime = time;
            	startKey = rndKey;
            	startValue = value;
            }
        	
        	engine.put(rndKey, rndKey, time, value.getBytes());
        	
        	map.put(key,value);
        	
            if(rndKey.equals(startKey)){
            	lastValue = value;
            	lastTime = time;
            }
        }

        
        System.out.println("start value:"+new String(engine.get(startKey, startKey, startTime)));

        ISeekIterator<InternalKey, byte[]> iterator = engine.iterator();

        iterator.seek(startKey, startKey, startTime);
        int count = 0;
        while(iterator.hasNext()){
        	iterator.next();
        	if(iterator.value()!=null)
        	System.out.println(++count+":"+iterator.time()+":"+iterator.table()+":"+new String(iterator.value()));
        }
        
        System.out.println("map size:"+map.size());
        System.out.println("last enging value:"+new String(engine.get(startKey, startKey, lastTime)));
        System.out.println("last value:"+lastValue);
        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second single thread%n",
                (int) (INIT_COUNT * 2 * 1e6 / duration));

    }
}
