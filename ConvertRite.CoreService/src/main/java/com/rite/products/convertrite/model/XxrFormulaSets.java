package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_FORMULA_SETS")
public class XxrFormulaSets {

	@Column(name = "FORMULA_SET_ID")
	private long formulaSetId;
	@Id
	@Column(name = "FORMULA_SET_NAME")
	private String formulaSetName;
	@Column(name = "DESCRIPTION")
	private String description;
	@Column(name = "POD_ID")
	private long podId;
	@Column(name = "PROJECT_ID")
	private long projectId;
	@Column(name = "OBJECT_ID")
	private long objectId;
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;
	/*
	 * @Column(name = "BU") private String bu;
	 */
	@Column(name = "CLOUD_COLUMN")
	private String cloudColumn;
	@Column(name = "FORMULA_TYPE")
	private String formulaType;
	@Column(name = "FORMULA_VALUE")
	private String formulaValue;
	@Column(name="SQLQUERY")
	private String sqlQuery;

	
	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public long getFormulaSetId() {
		return formulaSetId;
	}

	public void setFormulaSetId(long formulaSetId) {
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

	public long getPodId() {
		return podId;
	}

	public void setPodId(long podId) {
		this.podId = podId;
	}

	public long getProjectId() {
		return projectId;
	}

	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
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
