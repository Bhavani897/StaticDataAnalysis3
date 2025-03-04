package com.rite.products.convertrite.po;

public class SourceColumnsPo {
	
	private long columnId;
	private String columnName;
	
	public SourceColumnsPo(long columnId, String columnName) {
		super();
		this.columnId = columnId;
		this.columnName = columnName;
	}

	public long getColumnId() {
		return columnId;
	}

	/*
	 * public void setColumnId(long columnId) { this.columnId = columnId; }
	 */

	public String getColumnName() {
		return columnName;
	}

	/*
	 * public void setColumnName(String columnName) { this.columnName = columnName;
	 * }
	 */
	
	
	

}
