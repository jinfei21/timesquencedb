package com.ctriposs.tsdb;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public class DBEnginePutTest {

    private static final String TEST_DIR = "d:\\tsdb_test\\put_test";
    private static final int INIT_COUNT = 10*1000*1000;
    private static DBEngine engine;

    public static void main(String[] args) throws IOException  {


        DBConfig config = new DBConfig(TEST_DIR);
        engine = new DBEngine(config);
        
        String[] str = new String[]{"a","b","c","d","e","f","g"};
        Random random = new Random();
        long start = System.nanoTime();
        
        String data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        
        Map<Long,String> map = new LinkedHashMap<Long,String>();
        
        for (int i = 0; i < 2 * INIT_COUNT; i++) {
        	int n = random.nextInt(7);

        	long l = System.currentTimeMillis();
        	String d = data+i;
        	engine.put(str[n], str[n], l, d.getBytes());
        	map.put(l,str[n] + "-" + d);
        }
        
        for(Entry<Long,String> entry:map.entrySet()){
        	String d[] = entry.getValue().split("-");
        	
        	byte[] s = engine.get(d[0],d[0],entry.getKey());
        	if(s != null){
        		String dd = new String(s);
        		if(d[1].equals(dd)){
        			System.out.println("OK");
        		}else{
        			System.out.print("error "+entry.getValue()+"---");
        			System.out.print(d[1]+"--");
        			System.out.println(dd);
        		}
        	}else{
        		System.out.println("not found "+entry.getValue()+"-"+entry.getKey());
        	}
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second single thread%n",
                (int) (INIT_COUNT * 2 * 1e6 / duration));
		
	}
}
