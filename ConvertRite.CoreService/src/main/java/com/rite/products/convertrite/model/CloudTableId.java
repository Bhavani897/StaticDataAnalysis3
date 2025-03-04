package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class CloudTableId implements Serializable{
	
	
	//private int applicationId;
	
	private static final long serialVersionUID = 1L;

	private long tableId;
	
	@Column(name = "TABLE_NAME")
	private String tableName;

	/*
	 * public int getApplicationId() { return applicationId; } public void
	 * setApplicationId(int applicationId) { this.applicationId = applicationId; }
	 */
	public long getTableId() {
		return tableId;
	}
	public void setTableId(long tableId) {
		this.tableId = tableId;
	}
    
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
