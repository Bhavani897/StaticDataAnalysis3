package com.rite.products.convertrite.po;

public class SaveDateConfigurationReqPo {
	private Long id;
	private Long podId;
	private Long projectId;
	private Long objectId;
	private Long parentObjectId;
	private String sourceDateFormat;
	private String cloudDateFormat;

	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
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

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
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
