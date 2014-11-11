package com.ctriposs.tsdb.test;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import com.ctriposs.tsdb.DBConfig;
import com.ctriposs.tsdb.DBEngine;

public class DBEngineCompactFunctionTest {

    private static final String TEST_DIR = "d://tsdb_test/compact/";
    private static final String[] TABLE_NAME_ARRAY = new String[]{"a", "b", "c", "d", "e", "f", "g"};
    private static final String[] COLUMN_NAME_ARRAY = new String[]{"A", "B", "C", "D", "E", "F", "G"};

    private static DBEngine engine;

    public static void main(String[] args) throws IOException {

        DBConfig config = new DBConfig(TEST_DIR);
        engine = new DBEngine(config);
        Random random = new Random();

        System.out.println("Start from date " + System.currentTimeMillis());
        for (long counter = 0;; counter++) {
            engine.put(TABLE_NAME_ARRAY[random.nextInt(TABLE_NAME_ARRAY.length)], COLUMN_NAME_ARRAY[random.nextInt(COLUMN_NAME_ARRAY.length)], System.currentTimeMillis(), "fdsafasdfasdfasdfsdafsdafasdfasdfasdfasfsda".getBytes());

            if (counter  % 1000000 == 0) {
                System.out.println("Current date:" + new Date());
                System.out.println("counter:     " + counter);
                System.out.println("store        " + engine.getStoreCounter(0));
                System.out.println("store error  " + engine.getStoreErrorCounter(0));

                System.out.println();
                System.out.println();
            }

        }
    }
}
