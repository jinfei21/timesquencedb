package com.ctriposs.leveldb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class ByteUtil {

	public static byte[] toBytes(int n) {
		byte[] bytes = new byte[4];
		bytes[3] = (byte) (n & 0xff);
		bytes[2] = (byte) (n >> 8 & 0xff);
		bytes[1] = (byte) (n >> 16 & 0xff);
		bytes[0] = (byte) (n >> 24 & 0xff);
		return bytes;
	}
	
	public static byte[] toBytes(byte b) {
		byte[] bytes = new byte[1];
		bytes[0] = b;
		return bytes;
	}

	public static byte[] toBytes(long n) {

		byte[] bytes = new byte[8];
		bytes[7] = (byte) (n & 0xff);
		bytes[6] = (byte) (n >> 8 & 0xff);
		bytes[5] = (byte) (n >> 16 & 0xff);
		bytes[4] = (byte) (n >> 24 & 0xff);
		bytes[3] = (byte) (n >> 32 & 0xff);
		bytes[2] = (byte) (n >> 40 & 0xff);
		bytes[1] = (byte) (n >> 48 & 0xff);
		bytes[0] = (byte) (n >> 56 & 0xff);
		return bytes;

	}

	public static byte[] toBytes(short n) {
		byte[] bytes = new byte[2];
		bytes[1] = (byte) (n & 0xff);
		bytes[0] = (byte) ((n >> 8) & 0xff);
		return bytes;
	}

    public static byte[] ToBytes(Object o) throws IOException {
        if (o instanceof String) {
            return ((String) o).getBytes();
        } else if (o instanceof byte[]) {
            return ((byte[]) o);
        } else{
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(o);
            byte[] bytes = bos.toByteArray();
            bos.close();
            return bytes;
        }
    }

	public static short ToShort(byte[] bytes) {
		return (short) (bytes[1] & 0xff | (bytes[0] & 0xff) << 8);
	}

	public static int ToInt(byte bytes[]) {
		return bytes[3] & 0xff | (bytes[2] & 0xff) << 8
				| (bytes[1] & 0xff) << 16 | (bytes[0] & 0xff) << 24;
	}

	public static long ToLong(byte[] bytes) {
		return ((((long) bytes[0] & 0xff) << 56)
				| (((long) bytes[1] & 0xff) << 48)
				| (((long) bytes[2] & 0xff) << 40)
				| (((long) bytes[3] & 0xff) << 32)
				| (((long) bytes[4] & 0xff) << 24)
				| (((long) bytes[5] & 0xff) << 16)
				| (((long) bytes[6] & 0xff) << 8) | (((long) bytes[7] & 0xff) << 0));
	}

    public static int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}
