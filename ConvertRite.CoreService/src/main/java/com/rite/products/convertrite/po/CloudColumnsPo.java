package com.rite.products.convertrite.po;

public class CloudColumnsPo {
	
	private Long columnId;
	private String columnName;
	
	public CloudColumnsPo(Long columnId, String columnName) {
		super();
		this.columnId = columnId;
		this.columnName = columnName;
	}

	public Long getColumnId() {
		return columnId;
	}

	public void setColumnId(Long columnId) {
		this.columnId = columnId;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	
	
	

}
