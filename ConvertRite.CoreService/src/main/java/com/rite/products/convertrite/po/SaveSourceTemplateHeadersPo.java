package com.rite.products.convertrite.po;

import java.io.Serializable;

public class SaveSourceTemplateHeadersPo implements Serializable {

	private static final long serialVersionUID = 1L;

	private String templateCode;
	private Long objectId;
	private Long parentObjectId;
	private String templateName;
	private Long projectId;
	private Long templateId;
	private Long metaDataTableId;
	private String stagingTableName;
	private String viewName;
	private String normalizeDataFlag;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;

	
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

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public Long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Long templateId) {
		this.templateId = templateId;
	}

	public Long getMetaDataTableId() {
		return metaDataTableId;
	}

	public void setMetaDataTableId(Long metaDataTableId) {
		this.metaDataTableId = metaDataTableId;
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
