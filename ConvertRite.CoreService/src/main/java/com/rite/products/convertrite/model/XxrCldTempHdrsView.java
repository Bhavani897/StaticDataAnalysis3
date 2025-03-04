package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_CLD_TEMP_HDRS_V")
public class XxrCldTempHdrsView {
	
	@Id
	@Column(name = "TEMPLATE_ID")
	private long templateId;
	@Column(name = "TEMPLATE_NAME")
	private String templateName;
	/*
	 * @Column(name = "POD_ID") private Long podId;
	 */
	@Column(name = "POD_NAME")
	private String podName;
	@Column(name = "PROJECT_ID")
	private Long projectId;
	@Column(name = "PROJECT_NAME")
	private String projectName;
	@Column(name = "PARENT_OBJECT_ID")
	private Long parentObjectId;
	@Column(name = "PARENT_OBJECT_NAME")
	private String parentObjectCode;
	@Column(name = "OBJECT_ID")
	private Long objectId;
	@Column(name = "OBJECT_NAME")
	private String objectCode;
	@Column(name = "METADATA_TABLE_ID")
	private Long metaDataTableId;
	@Column(name = "METADATA_TABLE")
	private String metaDataTableName;
	@Column(name = "SRC_TEMPLATE_ID")
	private Long sourceTemplateId;
	@Column(name = "SOURCE_TEMPLATE_NAME")
	private String sourceTemplateName;
	@Column(name = "STAGING_TABLE_NAME")
	private String stagingTableName;
	@Column(name = "VIEW_NAME")
	private String viewName;
	/*
	 * @Column(name = "BU_SPECIFIC") private String buSpecific;
	 * 
	 * @Column(name = "BU") private String bu;
	 */
	@Column(name = "VERSION")
	private String version;
	@Column(name="ROLE_ID")
	private Long roleId;
	@Column(name="ATTRIBUTE1")
	private String attribute1;
	@Column(name="PRIMARY_TEMPLATE_FLAG")
	private String primaryTemplateFlag;
	@Column(name="TEMPLATE_CODE")
	private String templateCode;
	

	public String getPrimaryTemplateFlag() {
		return primaryTemplateFlag;
	}

	public void setPrimaryTemplateFlag(String primaryTemplateFlag) {
		this.primaryTemplateFlag = primaryTemplateFlag;
	}

	public String getTemplateCode() {
		return templateCode;
	}

	public void setTemplateCode(String templateCode) {
		this.templateCode = templateCode;
	}

	public String getAttribute1() {
		return attribute1;
	}

	public void setAttribute1(String attribute1) {
		this.attribute1 = attribute1;
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
	 * public Long getPodId() { return podId; }
	 * 
	 * public void setPodId(Long podId) { this.podId = podId; }
	 */

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

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
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

	public Long getSourceTemplateId() {
		return sourceTemplateId;
	}

	public void setSourceTemplateId(Long sourceTemplateId) {
		this.sourceTemplateId = sourceTemplateId;
	}

	public String getSourceTemplateName() {
		return sourceTemplateName;
	}

	public void setSourceTemplateName(String sourceTemplateName) {
		this.sourceTemplateName = sourceTemplateName;
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

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

}
