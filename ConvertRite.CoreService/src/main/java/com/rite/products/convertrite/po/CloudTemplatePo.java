package com.rite.products.convertrite.po;

public class CloudTemplatePo {
	private Long templateId;
	private Long tableId;
	private String templateName;
	private String templateType;
	private String objectCode;
	private String parentObjectCode;
	private String cloudDataTableName;
	private String sourceHeaderTemplate;
	private Integer bu;
	private String buSpecific;
	private String projectName;
	private Long podId;
	private String tableName;
	
	public Long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Long templateId) {
		this.templateId = templateId;
	}

	public Long getTableId() {
		return tableId;
	}

	public void setTableId(Long tableId) {
		this.tableId = tableId;
	}

	public Integer getBu() {
		return bu;
	}

	public void setBu(Integer bu) {
		this.bu = bu;
	}

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	private String viewName;

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

	public String getCloudDataTableName() {
		return cloudDataTableName;
	}

	public void setCloudDataTableName(String cloudDataTableName) {
		this.cloudDataTableName = cloudDataTableName;
	}

	public String getSourceHeaderTemplate() {
		return sourceHeaderTemplate;
	}

	public void setSourceHeaderTemplate(String sourceHeaderTemplate) {
		this.sourceHeaderTemplate = sourceHeaderTemplate;
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



	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

}
