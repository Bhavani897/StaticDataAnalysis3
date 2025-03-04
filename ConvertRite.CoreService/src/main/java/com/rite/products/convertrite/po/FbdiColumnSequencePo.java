package com.rite.products.convertrite.po;

public class FbdiColumnSequencePo {

	private String fbdiColumnName;
	private String databaseColumn;
	private Integer sequence;
	private String Required;

	public String getRequired() {
		return Required;
	}

	public void setRequired(String required) {
		Required = required;
	}

	public String getFbdiColumnName() {
		return fbdiColumnName;
	}

	public void setFbdiColumnName(String fbdiColumnName) {
		this.fbdiColumnName = fbdiColumnName;
	}

	public String getDatabaseColumn() {
		return databaseColumn;
	}

	public void setDatabaseColumn(String databaseColumn) {
		this.databaseColumn = databaseColumn;
	}

	public Integer getSequence() {
		return sequence;
	}

	public void setSequence(Integer sequence) {
		this.sequence = sequence;
	}

}
