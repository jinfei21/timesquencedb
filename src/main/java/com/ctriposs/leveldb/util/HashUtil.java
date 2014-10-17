package com.ctriposs.leveldb.util;

public class HashUtil {

    public static int JSHash(byte[] bytes) {
    
        int hash = 1315423911;

        for (byte aByte : bytes) {
            hash ^= ((hash << 5) + aByte + (hash >> 2));
        }

        return (hash & Integer.MAX_VALUE);
    }

    public static int DJBHash(byte[] bytes) {

        int hash = 5381;

        for (byte aByte : bytes) {
            hash = ((hash << 5) + hash) + aByte;
        }

        return hash;
    }

}
