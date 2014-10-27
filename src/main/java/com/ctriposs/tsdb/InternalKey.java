package com.ctriposs.tsdb;


public class InternalKey {

	private int tableCode;
	private int columnCode;
	private long time;
	
	public InternalKey(int tableCode, int columnCode, long time) {
		this.tableCode = tableCode;
		this.columnCode = columnCode;
		this.time = time;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public int getTableCode() {
		return tableCode;
	}

	public void setTableCode(int tableCode) {
		this.tableCode = tableCode;
	}

	public int getColumnCode() {
		return columnCode;
	}

	public void setColumnCode(int columnCode) {
		this.columnCode = columnCode;
	}

}
