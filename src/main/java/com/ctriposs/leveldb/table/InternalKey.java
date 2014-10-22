package com.ctriposs.leveldb.table;

import com.ctriposs.leveldb.Constant;
import com.ctriposs.leveldb.storage.SliceOutput;
import com.ctriposs.leveldb.util.SequenceUtil;
import com.ctriposs.leveldb.util.SliceUtil;
import com.google.common.base.Preconditions;

public class InternalKey {

    private final Slice userKey;
    private final long sequence;
    private final ValueType valueType;

    public InternalKey(Slice userKey, long sequence, ValueType valueType)
    {
        Preconditions.checkNotNull(userKey, "userKey is null");
        Preconditions.checkArgument(sequence >= 0, "sequenceNumber is negative");
        Preconditions.checkNotNull(valueType, "valueType is null");

        this.userKey = userKey;
        this.sequence = sequence;
        this.valueType = valueType;
    }

    public InternalKey(Slice data)
    {
        Preconditions.checkNotNull(data, "data is null");
        Preconditions.checkArgument(data.length() >= Constant.SIZE_OF_LONG, "data must be at least %s bytes", Constant.SIZE_OF_LONG);
        this.userKey = SliceUtil.getUserKey(data);
        long packedSequenceAndType = data.getLong(data.length() - Constant.SIZE_OF_LONG);
        this.sequence = SequenceUtil.unpackSequence(packedSequenceAndType);
        this.valueType = SequenceUtil.unpackValueType(packedSequenceAndType);
    }
    
    public Slice encode(){
        Slice slice = SliceUtil.allocate(userKey.length() + Constant.SIZE_OF_LONG);
        SliceOutput sliceOutput = slice.output();
        sliceOutput.writeBytes(userKey.getData());
        sliceOutput.writeLong(SequenceUtil.packSequenceAndValueType(sequence, valueType));
        return slice;
    }

    public InternalKey(byte[] data)
    {
    	this(new Slice(data));
    }

    public Slice getUserKey()
    {
        return userKey;
    }

    public long getSequence()
    {
        return sequence;
    }

    public ValueType getValueType()
    {
        return valueType;
    }

    @Override
    public String toString(){
    	final StringBuilder sb = new StringBuilder();
    	sb.append("InternalKey");
    	sb.append("{key=").append(getUserKey());
    	sb.append(",sequence=").append(getSequence());
    	sb.append(",valueType=").append(getValueType());
    	sb.append('}');
    	return sb.toString();
    }
    
    @Override
    public boolean equals(Object o){
    	if(this == o){
    		return true;
    	}
    	
    	if(o==null||getClass()!=o.getClass()){
    		return false;
    	}
    	
    	InternalKey other = (InternalKey) o;
    	
    	if(sequence!=other.sequence){
    		return false;
    	}
    	
    	if(valueType != other.valueType){
    		return false;
    	}
    	
    	if(userKey != null?!userKey.equals(other.userKey):other.userKey!=null){
    		return false;
    	}
    	
    	return true;
    }
}
