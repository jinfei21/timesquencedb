package com.ctriposs.leveldb;

import java.io.Closeable;

public interface IWriteBatch extends Closeable {

    public IWriteBatch put(byte[] key, byte[] value);
    public IWriteBatch delete(byte[] key);
}