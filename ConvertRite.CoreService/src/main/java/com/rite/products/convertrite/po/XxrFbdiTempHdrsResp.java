package com.rite.products.convertrite.po;

public class XxrFbdiTempHdrsResp {

	private long fbdiTemplateId;
	private String fbdiTemplateName;
	private long podId;
	private String podName;
	private long projectId;
	private String projectName;
	private long parentObjectId;
	private String parentObjectCode;
	private long objectId;
	private String objectCode;
	private String version;
	private String sheetName;
	private String api;
	
	
	
	public String getApi() {
		return api;
	}

	public void setApi(String api) {
		this.api = api;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

	public long getProjectId() {
		return projectId;
	}

	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public long getFbdiTemplateId() {
		return fbdiTemplateId;
	}

	public void setFbdiTemplateId(long fbdiTemplateId) {
		this.fbdiTemplateId = fbdiTemplateId;
	}

	public String getFbdiTemplateName() {
		return fbdiTemplateName;
	}

	public void setFbdiTemplateName(String fbdiTemplateName) {
		this.fbdiTemplateName = fbdiTemplateName;
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

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

}
