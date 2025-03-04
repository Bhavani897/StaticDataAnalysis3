package com.rite.products.convertrite.po;

public class SaveFormulaSetHeaderPo {
	private Long formulaSetId;
	private String formulaSetName;
	private String description;
	private Long podId;
	private Long projectId;
	private Long objectId;
	private Long parentObjectId;
	/* private String bu; */
	private String cloudColumn;
	private String formulaType;
	private String formulaValue;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;
	private String sqlQuery;

	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
		this.objectId = objectId;
	}

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getAttribute1() {
		return attribute1;
	}

	public void setAttribute1(String attribute1) {
		this.attribute1 = attribute1;
	}

	public String getAttribute2() {
		return attribute2;
	}

	public void setAttribute2(String attribute2) {
		this.attribute2 = attribute2;
	}

	public String getAttribute3() {
		return attribute3;
	}

	public void setAttribute3(String attribute3) {
		this.attribute3 = attribute3;
	}

	public String getAttribute4() {
		return attribute4;
	}

	public void setAttribute4(String attribute4) {
		this.attribute4 = attribute4;
	}

	public String getAttribute5() {
		return attribute5;
	}

	public void setAttribute5(String attribute5) {
		this.attribute5 = attribute5;
	}

	public Long getFormulaSetId() {
		return formulaSetId;
	}

	public void setFormulaSetId(Long formulaSetId) {
		this.formulaSetId = formulaSetId;
	}

	public String getFormulaSetName() {
		return formulaSetName;
	}

	public void setFormulaSetName(String formulaSetName) {
		this.formulaSetName = formulaSetName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	/*
	 * public String getBu() { return bu; }
	 * 
	 * public void setBu(String bu) { this.bu = bu; }
	 */

	public String getCloudColumn() {
		return cloudColumn;
	}

	public void setCloudColumn(String cloudColumn) {
		this.cloudColumn = cloudColumn;
	}

	public String getFormulaType() {
		return formulaType;
	}

	public void setFormulaType(String formulaType) {
		this.formulaType = formulaType;
	}

	public String getFormulaValue() {
		return formulaValue;
	}

	public void setFormulaValue(String formulaValue) {
		this.formulaValue = formulaValue;
	}

}
