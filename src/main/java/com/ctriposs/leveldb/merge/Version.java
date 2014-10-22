package com.ctriposs.leveldb.merge;

import java.util.ArrayList;
import java.util.List;

import com.ctriposs.leveldb.Constant;
import com.ctriposs.leveldb.storage.FileMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class Version {

	private final List<Level> levels;
	
	
	public Version(){
		
		
		Builder<Level> builder = ImmutableList.builder();
		for(int i=0;i<Constant.MAX_LEVELS;i++){
			List<FileMeta> files = new ArrayList<FileMeta>();
			builder.add(new Level(files,i));
		}
		this.levels = builder.build();
	}
	
	public int getFileCountInLevel(int level){
		return levels.get(level).getFileMetas().size();
	}
	
}
