package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_SRC_TEMP_HDRS_V")
public class XxrSrcTempHdrsView {

	@Id
	@Column(name = "TEMPLATE_ID")
	private long templateId;
	@Column(name = "TEMPLATE_NAME")
	private String templateName;
	/*
	 * @Column(name = "POD_ID") private long podId;
	 * 
	 * @Column(name = "POD_NAME") private String podName;
	 */
	
	@Column(name="TEMPLATE_CODE")
	private String templateCode;
	@Column(name="NORMALIZE_DATA_FLAG")
	private String normalizeDataFlag;
	@Column(name = "PROJECT_ID")
	private long projectId;
	@Column(name = "PROJECT_NAME")
	private String projectName;
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;
	@Column(name = "PARENT_OBJECT_NAME")
	private String parentObjectCode;
	@Column(name = "OBJECT_ID")
	private long objectId;
	@Column(name = "OBJECT_NAME")
	private String objectCode;
	@Column(name = "METADATA_TABLE_ID")
	private Long metaDataTableId;
	@Column(name = "METADATA_TABLE")
	private String metaDataTableName;
	@Column(name = "STAGING_TABLE_NAME")
	private String stagingTableName;
	@Column(name = "VIEW_NAME")
	private String viewName;
	/*
	 * @Column(name = "BU_SPECIFIC") private String buSpecific;
	 * 
	 * @Column(name = "BU") private String bu;
	 */
	@Column(name = "ATTRIBUTE1")
	private String attribute1;
	@Column(name = "ATTRIBUTE2")
	private String attribute2;
	@Column(name = "ATTRIBUTE3")
	private String attribute3;
	@Column(name = "ATTRIBUTE4")
	private String attribute4;
	@Column(name = "ATTRIBUTE5")
	private String attribute5;
	@Column(name="ROLE_ID")
	private Long roleId;
	
	

	public String getTemplateCode() {
		return templateCode;
	}

	public void setTemplateCode(String templateCode) {
		this.templateCode = templateCode;
	}

	public String getNormalizeDataFlag() {
		return normalizeDataFlag;
	}

	public void setNormalizeDataFlag(String normalizeDataFlag) {
		this.normalizeDataFlag = normalizeDataFlag;
	}

	public Long getRoleId() {
		return roleId;
	}

	public void setRoleId(Long roleId) {
		this.roleId = roleId;
	}

	public long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(long templateId) {
		this.templateId = templateId;
	}

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	/*
	 * public long getPodId() { return podId; }
	 * 
	 * public void setPodId(long podId) { this.podId = podId; }
	 * 
	 * public String getPodName() { return podName; }
	 * 
	 * public void setPodName(String podName) { this.podName = podName; }
	 */

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

	public Long getMetaDataTableId() {
		return metaDataTableId;
	}

	public void setMetaDataTableId(Long metaDataTableId) {
		this.metaDataTableId = metaDataTableId;
	}

	public String getMetaDataTableName() {
		return metaDataTableName;
	}

	public void setMetaDataTableName(String metaDataTableName) {
		this.metaDataTableName = metaDataTableName;
	}

	public String getStagingTableName() {
		return stagingTableName;
	}

	public void setStagingTableName(String stagingTableName) {
		this.stagingTableName = stagingTableName;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

	/*
	 * public String getBuSpecific() { return buSpecific; }
	 * 
	 * public void setBuSpecific(String buSpecific) { this.buSpecific = buSpecific;
	 * }
	 * 
	 * public String getBu() { return bu; }
	 * 
	 * public void setBu(String bu) { this.bu = bu; }
	 */

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

}
