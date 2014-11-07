package com.ctriposs.tsdb;

import java.io.IOException;

public interface ILogWriter {

    void close() throws IOException;

    void add(int code, long time, byte[] value) throws IOException;

    void add(String name, short code) throws IOException;
    
    int getLength();
    
    String getName();
}
