package com.ctriposs.leveldb.table;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;

import com.ctriposs.leveldb.ISeekIterable;
import com.ctriposs.leveldb.iterator.TableIteractor;
import com.google.common.io.Closeables;

public abstract class Table implements ISeekIterable<Slice, Slice>{
    protected final String name;
    protected final FileChannel fileChannel;
	
    public Table(String name, FileChannel fileChannel) throws IOException{
    
    	this.name = name;
    	this.fileChannel = fileChannel;
    }
	
    public Callable<?> closer() {
        return new Closer(fileChannel);
    }
    
    @Override
    public TableIteractor iterator(){
    	return new TableIteractor(this);
    }

    private static class Closer implements Callable<Void>
    {
        private final Closeable closeable;

        public Closer(Closeable closeable)
        {
            this.closeable = closeable;
        }

        public Void call()
        {    
			Closeables.closeQuietly(closeable);
            return null;
        }
    }
}
