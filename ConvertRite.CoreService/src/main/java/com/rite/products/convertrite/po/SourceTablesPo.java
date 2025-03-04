package com.rite.products.convertrite.po;

public class SourceTablesPo {

	private Long tableId;
	private String tableName;

	public SourceTablesPo(Long tableId, String tableName) {
		super();
		this.tableId = tableId;
		this.tableName = tableName;
	}

	public Long getTableId() {
		return tableId;
	}

	public void setTableId(Long tableId) {
		this.tableId = tableId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
