package com.ctriposs.leveldb.merge;

import java.util.Comparator;
import java.util.List;

import com.ctriposs.leveldb.storage.FileMeta;

import static com.google.common.collect.Lists.newArrayList;


public class Level {

	private final List<FileMeta> fileMetas;
	private final int levelNumer;
	
	
	private static final Comparator<FileMeta> fileMetaComparator = new Comparator<FileMeta>() {
		@Override
		public int compare(FileMeta o1, FileMeta o2) {
			return (int)(o1.getNumber() - o2.getNumber());
		}
	};
	
	
	public Level(List<FileMeta> fileMetas,int levelNumer){
		this.fileMetas = fileMetas;
		this.levelNumer = levelNumer;
		
	}
	
	public int getLevelNumber(){
		return levelNumer;
	}
	
	public List<FileMeta> getFileMetas(){
		return fileMetas;
	}
	
}
