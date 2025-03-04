package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_CLOUD_COLUMNS")
public class XxrCloudColumns implements Serializable {

	private static final long serialVersionUID = 1L;

	@Column(name = "TABLE_ID")
	private long tableId;

	@Column(name = "COLUMN_NAME")
	private String columnName;

	@Id
	@Column(name = "COLUMN_ID")
	private long columnId;

	@Column(name = "OBJECT_ID")
	private long objectId;

	@Column(name = "NULL_ALLOWED_FLAG")
	private String nullAllowedFlag;

	@Column(name = "COLUMN_TYPE")
	private String columnType;

	@Column(name = "DESCRIPTION")
	private String description;

	@Column(name = "WIDTH")
	private String width;

	public String getWidth() {
		return width;
	}

	public void setWidth(String width) {
		this.width = width;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getNullAllowedFlag() {
		return nullAllowedFlag;
	}

	public void setNullAllowedFlag(String nullAllowedFlag) {
		this.nullAllowedFlag = nullAllowedFlag;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}

	public long getColumnId() {
		return columnId;
	}

	public void setColumnId(long columnId) {
		this.columnId = columnId;
	}

	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

}
