package com.ctriposs.tsdb;

import java.io.IOException;

public interface ILogReader {

    void close() throws IOException;

    void add(int code, long time, byte[] value) throws IOException;

    void add(String name, short code) throws IOException;
    
    String getName();
}
