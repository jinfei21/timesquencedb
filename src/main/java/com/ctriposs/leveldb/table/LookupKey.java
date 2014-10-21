package com.ctriposs.leveldb.table;

public class LookupKey {

	private final InternalKey key;
	
	public LookupKey(Slice internalKey,long sequence){
		this.key = new InternalKey(internalKey,sequence,ValueType.VALUE);
	}
	
	public InternalKey getInternalKey(){
		return key;
	}
	
	public Slice getUserKey(){
		return key.getUserKey();
	}
	
	@Override
	public String toString(){
		return key.toString();
	}
}
