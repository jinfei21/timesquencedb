package com.ctriposs.leveldb.util;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

import sun.nio.ch.FileChannelImpl;

import com.google.common.base.Throwables;

public class ByteBufferUtil {

    private static final Method unmap;
    static {
        Method x;
        try {
            x = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        x.setAccessible(true);
        unmap = x;
    }

    public static void unmap(MappedByteBuffer buffer)
    {
        try {
            unmap.invoke(null, buffer);
        }
        catch (Exception ignored) {
            throw Throwables.propagate(ignored);
        }
    }


}