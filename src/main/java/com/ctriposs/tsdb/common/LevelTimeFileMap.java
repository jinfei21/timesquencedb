package com.ctriposs.tsdb.common;

import com.ctriposs.tsdb.storage.FileMeta;

import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;

public class LevelTimeFileMap extends ConcurrentSkipListMap<Long, Queue<FileMeta>> {
}
