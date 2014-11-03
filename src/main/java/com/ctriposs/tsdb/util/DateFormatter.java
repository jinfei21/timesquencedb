package com.ctriposs.tsdb.util;

import com.ctriposs.tsdb.table.MemTable;

public class DateFormatter {

    public static long oneMinuteFormatter(long time) {
        return time/MemTable.MINUTE*MemTable.MINUTE;
    }
}
