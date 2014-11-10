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


        DBConfig config = new DBConfig(TEST_DIR);
        engine = new DBEngine(config);

        String[] str = new String[]{"a","b","c","d","e","f","g"};
        Random random = new Random();
        long start = System.nanoTime();

        String data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Map<Long,String> map = new LinkedHashMap<Long,String>();

        long s = 0;
        String table = null;
        String last = null;
        long lt = 0;
        int sss = 0;
        
        for (int i = 0; i < 2*INIT_COUNT; i++) {
        	String n = String.valueOf(random.nextInt(30000));

            long l = System.currentTimeMillis();
            if(i==0){
            	s = l;
            	table = n;
            }
        	String d = data+i;
        	engine.put(n, n, l, d.getBytes());
        	map.put(l,n + "-" + d);
            if(table.equals(n)){
            	last = d;
            	lt = l;
            	sss++;
            }
        }

        
        System.out.println(new String(engine.get(table, table, s)));

        ISeekIterator<InternalKey, byte[]> iterator = engine.iterator();

        iterator.seek(table, table, s);
        int n = 0;
        while(iterator.hasNext()){
        	iterator.next();
        	if(iterator.value()!=null)
        	System.out.println(++n+":"+iterator.time()+":"+iterator.table()+":"+new String(iterator.value()));
        }
        
        System.out.println(lt+":"+table+":last:"+last);
        System.out.println(new String(engine.get(table, table, lt)));
        System.out.println(sss);
        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second single thread%n",
                (int) (INIT_COUNT * 2 * 1e6 / duration));

    }
}
