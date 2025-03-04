package com.rite.products.convertrite.po;

public class SaveCloudTemplateHeadersPo {

	private Long templateId;
	private String templateName;
	//private Long podId;
	private Long projectId;
	private Long parentObjectId;
	private Long objectId;
	private Long metaDataTableId;
	private Long sourceTemplateId;
	private String stagingTableName;
	private String viewName;
	/*
	 * private String bu; private String buSpecific;
	 */
	private String templateCode;
	private String primaryTemplateFlag;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;
	private String version;
	
	
	public String getTemplateCode() {
		return templateCode;
	}

	public void setTemplateCode(String templateCode) {
		this.templateCode = templateCode;
	}

	public String getPrimaryTemplateFlag() {
		return primaryTemplateFlag;
	}

	public void setPrimaryTemplateFlag(String primaryTemplateFlag) {
		this.primaryTemplateFlag = primaryTemplateFlag;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public Long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Long templateId) {
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
	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
		this.objectId = objectId;
	}

	public Long getMetaDataTableId() {
		return metaDataTableId;
	}

	public void setMetaDataTableId(Long metaDataTableId) {
		this.metaDataTableId = metaDataTableId;
	}

	public Long getSourceTemplateId() {
		return sourceTemplateId;
	}

	public void setSourceTemplateId(Long sourceTemplateId) {
		this.sourceTemplateId = sourceTemplateId;
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
	 * public String getBu() { return bu; }
	 * 
	 * public void setBu(String bu) { this.bu = bu; }
	 * 
	 * public String getBuSpecific() { return buSpecific; }
	 * 
	 * public void setBuSpecific(String buSpecific) { this.buSpecific = buSpecific;
	 * }
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
