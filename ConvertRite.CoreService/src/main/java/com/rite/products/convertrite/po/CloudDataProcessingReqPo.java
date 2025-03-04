package com.rite.products.convertrite.po;

public class CloudDataProcessingReqPo {
	private String sqlQuery;
	private String description;
	private String lookUpFlag;
	private String scheduledJobCall;
	private Integer priority;
	private Long podId;
	private Long projectId;
	private String metaData;
	private String tableName;
	private String extractionFlag;
	private String requestId;

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getMetaData() {
		return metaData;
	}

	public void setMetaData(String metaData) {
		this.metaData = metaData;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getExtractionFlag() {
		return extractionFlag;
	}

	public void setExtractionFlag(String extractionFlag) {
		this.extractionFlag = extractionFlag;
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

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getLookUpFlag() {
		return lookUpFlag;
	}

	public void setLookUpFlag(String lookUpFlag) {
		this.lookUpFlag = lookUpFlag;
	}

	public String getScheduledJobCall() {
		return scheduledJobCall;
	}

	public void setScheduledJobCall(String scheduledJobCall) {
		this.scheduledJobCall = scheduledJobCall;
	}

}
