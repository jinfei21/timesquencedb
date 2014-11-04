package com.ctriposs.tsdb.util;

import com.ctriposs.tsdb.table.MemTable;

public class DateFormatter {

    public static long minuteFormatter(long time, int power) {
        long levelMinute = MemTable.MINUTE * power;
        return time/levelMinute*levelMinute;
    }
}
