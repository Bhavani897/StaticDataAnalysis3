package com.rite.products.convertrite.po;

public class DateConfigurationResPo {

	private long id;
	private long podId;
	private String podName;
	private long projectId;
	private String projectName;
	private long parentObjectId;
	private String parentObjectCode;
	private long objectId;
	private String objectCode;
	private String sourceDateFormat;
	private String cloudDateFormat;

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

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
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

	public String getSourceDateFormat() {
		return sourceDateFormat;
	}

	public void setSourceDateFormat(String sourceDateFormat) {
		this.sourceDateFormat = sourceDateFormat;
	}

	public String getCloudDateFormat() {
		return cloudDateFormat;
	}

	public void setCloudDateFormat(String cloudDateFormat) {
		this.cloudDateFormat = cloudDateFormat;
	}

}
