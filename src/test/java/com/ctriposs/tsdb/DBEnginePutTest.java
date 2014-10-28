package com.ctriposs.tsdb;

import java.io.IOException;
import java.util.Random;

public class DBEnginePutTest {

    private static final String TEST_DIR = "d://tsdb_test/put_test";
    private static final int INIT_COUNT = 10*1000*1000;
    private static DBEngine engine;

    public static void main(String[] args) throws IOException {


        DBConfig config = new DBConfig(TEST_DIR);
        engine = new DBEngine(config);
        
        String[] str = new String[]{"a","b","c"};
        Random random = new Random();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 2 * INIT_COUNT; i++) {
        	engine.put(str[random.nextInt(3)], str[random.nextInt(3)], System.currentTimeMillis(), "abd_tead".getBytes());
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second single thread%n",
                (int) (INIT_COUNT * 2 * 1e6 / duration));
		
	}
}
