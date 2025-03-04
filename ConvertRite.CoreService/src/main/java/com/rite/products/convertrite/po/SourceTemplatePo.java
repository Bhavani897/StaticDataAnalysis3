package com.rite.products.convertrite.po;

public class SourceTemplatePo {

	private Long templateId;
	private Long sourceTableId;
	private String templateName;
	private String templateType;
	private String objectCode;
	private String parentObjectCode;
	private Integer bu;
	private String buSpecific;
	private String projectName;
	private Long podId;
	private String sourceTableName;
	private String stagingTableName;
	private String viewName;

	public Long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Long templateId) {
		this.templateId = templateId;
	}

	public Long getSourceTableId() {
		return sourceTableId;
	}

	public void setSourceTableId(Long sourceTableId) {
		this.sourceTableId = sourceTableId;
	}

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	public String getTemplateType() {
		return templateType;
	}

	public void setTemplateType(String templateType) {
		this.templateType = templateType;
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

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public String getSourceTableName() {
		return sourceTableName;
	}

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
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

}
