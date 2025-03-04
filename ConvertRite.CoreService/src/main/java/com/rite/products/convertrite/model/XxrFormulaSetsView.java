package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_FORMULA_SETS_V")
public class XxrFormulaSetsView {

	@Id
	@Column(name = "FORMULA_SET_ID")
	private long formulaSetId;
	@Column(name = "FORMULA_SET_NAME")
	private String formulaSetName;
	@Column(name = "DESCRIPTION")
	private String description;
	@Column(name = "POD_ID")
	private long podId;
	@Column(name = "POD_NAME")
	private String podName;
	@Column(name = "PROJECT_ID")
	private long projectId;
	@Column(name = "PROJECT_NAME")
	private String projectName;
	@Column(name = "OBJECT_ID")
	private long objectId;
	@Column(name = "OBJECT_CODE")
	private String objectCode;
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;
	@Column(name = "PARENT_OBJECT_CODE")
	private String parentObjectCode;
	@Column(name = "CLOUD_COLUMN")
	private String cloudColumn;
	@Column(name = "FORMULA_TYPE")
	private String formulaType;
	@Column(name = "FORMULA_VALUE")
	private String formulaValue;
	@Column(name = "SQLQUERY")
	private String sqlQuery;
	@Column(name="ROLE_ID")
	private Long roleId;

	public Long getRoleId() {
		return roleId;
	}

	public void setRoleId(Long roleId) {
		this.roleId = roleId;
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

	public String getPodName() {
		return podName;
	}

	public void setPodName(String podName) {
		this.podName = podName;
	}

	public long getProjectId() {
		return projectId;
	}

	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

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

	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

}
