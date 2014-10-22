package com.ctriposs.leveldb;

import java.io.File;
import java.io.IOException;

import com.ctriposs.leveldb.table.Slice;

public interface ILogWriter
{
    boolean isClosed();

    void close() throws IOException;

    void delete()throws IOException;

    File getFile();

    long getFileNumber();

    void addRecord(Slice key,Slice value, boolean force)throws IOException;
}
