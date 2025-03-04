package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class ColumnPo {

	private long columnId;
	private String columnName;
	private String nullAllowedFlag;
	private String columnType;
	private String description;
	private String width;

	public ColumnPo(long columnId, String columnName, String nullAllowedFlag, String columnType) {
		super();
		this.columnId = columnId;
		this.columnName = columnName;
		this.nullAllowedFlag = nullAllowedFlag;
		this.columnType = columnType;
	}

	public ColumnPo(long columnId, String columnName, String nullAllowedFlag, String columnType, String description) {
		super();
		this.columnId = columnId;
		this.columnName = columnName;
		this.nullAllowedFlag = nullAllowedFlag;
		this.columnType = columnType;
		this.description = description;
	}

	public ColumnPo(long columnId, String columnName, String nullAllowedFlag, String columnType, String description,
					String width) {
		super();
		this.columnId = columnId;
		this.columnName = columnName;
		this.nullAllowedFlag = nullAllowedFlag;
		this.columnType = columnType;
		this.description = description;
		this.width = width;
	}

}
