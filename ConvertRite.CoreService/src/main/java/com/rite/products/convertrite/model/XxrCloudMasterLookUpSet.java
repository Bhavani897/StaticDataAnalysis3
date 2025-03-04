package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_CLOUD_MASTER_LOOKUP_SET")
public class XxrCloudMasterLookUpSet {
	@Id
	@Column(name="LOOKUP_SET_ID")
	private Long lookUpSetId;
	@Column(name="BU")
	private Integer bu;
	@Column(name="BU_SPECIFIC")
	private String buSpecific;
	@Column(name="SAAS_OBJECT_CODE")
	private String objectCode;
	@Column(name="SAAS_PARENT_OBJECT_CODE")
	private String parentObjectCode;
	@Column(name="COLUMN_ID")
	private Long columnId;
	@Column(name="COLUMN_NAME")
	private String columnName;
	@Column(name = "POD_ID")
	private Long podId;
	@Column(name = "POD_NAME")
	private String podName;
	@Column(name = "PROJECT_ID")
	private Long projectId;
	@Column(name = "PROJECT_NAME")
	private String projectName;
	
	

	public Long getLookUpSetId() {
		return lookUpSetId;
	}

	public void setLookUpSetId(Long lookUpSetId) {
		this.lookUpSetId = lookUpSetId;
	}

	public Integer getBu() {
		return bu;
	}

	public void setBu(Integer bu) {
		this.bu = bu;
	}

	public String getBuSpecific() {
		return buSpecific;
	}

	public void setBuSpecific(String buSpecific) {
		this.buSpecific = buSpecific;
	}

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
	}

	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
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

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public String getPodName() {
		return podName;
	}

	public void setPodName(String podName) {
		this.podName = podName;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

}
