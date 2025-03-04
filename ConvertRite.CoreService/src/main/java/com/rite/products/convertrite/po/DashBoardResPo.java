package com.rite.products.convertrite.po;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class DashBoardResPo {
	private long projectId;
	private String projectName;
	private Long taskId;
	private long seq;
	private long wbsId;
	private long podId;
	private String podName;
	private String taskNum;
	private String taskName;
	private long objectId;
	private String objectCode;
	private long parentObjectId;
	private String parentObjectCode;
	private String taskType;
	private String preReqTask;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date endDate;
	private Integer weightage;
	private Integer completePercentage;
	private Long legacyResourceId;
	private String legacyResource;
	private String taskStatus;
	private Long destinationResourceId;
	private String destinationResource;
	private Long taskOwnerId;
	private String taskOwner;
	private String completionFlag;
	private Long cloudResourceId;
	private String cloudResource;
	private Long integratorResourceId;
	private String integratorResource;
	private Long clientResourceId;
	private String clientResource;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date projectStartDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date projectEndDate;
	
	public Date getProjectStartDate() {
		return projectStartDate;
	}
	public void setProjectStartDate(Date projectStartDate) {
		this.projectStartDate = projectStartDate;
	}
	public Date getProjectEndDate() {
		return projectEndDate;
	}
	public void setProjectEndDate(Date projectEndDate) {
		this.projectEndDate = projectEndDate;
	}
	public long getProjectId() {
		return projectId;
	}
	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}
	public Long getTaskId() {
		return taskId;
	}
	public void setTaskId(Long taskId) {
		this.taskId = taskId;
	}
	public long getSeq() {
		return seq;
	}
	public void setSeq(long seq) {
		this.seq = seq;
	}
	public long getWbsId() {
		return wbsId;
	}
	public void setWbsId(long wbsId) {
		this.wbsId = wbsId;
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
	public String getTaskNum() {
		return taskNum;
	}
	public void setTaskNum(String taskNum) {
		this.taskNum = taskNum;
	}
	public String getTaskName() {
		return taskName;
	}
	public void setTaskName(String taskName) {
		this.taskName = taskName;
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
	public String getTaskType() {
		return taskType;
	}
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	public String getPreReqTask() {
		return preReqTask;
	}
	public void setPreReqTask(String preReqTask) {
		this.preReqTask = preReqTask;
	}
	public Date getStartDate() {
		return startDate;
	}
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	public Date getEndDate() {
		return endDate;
	}
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	public Integer getWeightage() {
		return weightage;
	}
	public void setWeightage(Integer weightage) {
		this.weightage = weightage;
	}
	public Integer getCompletePercentage() {
		return completePercentage;
	}
	public void setCompletePercentage(Integer completePercentage) {
		this.completePercentage = completePercentage;
	}
	public Long getLegacyResourceId() {
		return legacyResourceId;
	}
	public void setLegacyResourceId(Long legacyResourceId) {
		this.legacyResourceId = legacyResourceId;
	}
	public String getLegacyResource() {
		return legacyResource;
	}
	public void setLegacyResource(String legacyResource) {
		this.legacyResource = legacyResource;
	}
	public String getTaskStatus() {
		return taskStatus;
	}
	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}
	public Long getDestinationResourceId() {
		return destinationResourceId;
	}
	public void setDestinationResourceId(Long destinationResourceId) {
		this.destinationResourceId = destinationResourceId;
	}
	public String getDestinationResource() {
		return destinationResource;
	}
	public void setDestinationResource(String destinationResource) {
		this.destinationResource = destinationResource;
	}
	public Long getTaskOwnerId() {
		return taskOwnerId;
	}
	public void setTaskOwnerId(Long taskOwnerId) {
		this.taskOwnerId = taskOwnerId;
	}
	public String getTaskOwner() {
		return taskOwner;
	}
	public void setTaskOwner(String taskOwner) {
		this.taskOwner = taskOwner;
	}
	public String getCompletionFlag() {
		return completionFlag;
	}
	public void setCompletionFlag(String completionFlag) {
		this.completionFlag = completionFlag;
	}
	public Long getCloudResourceId() {
		return cloudResourceId;
	}
	public void setCloudResourceId(Long cloudResourceId) {
		this.cloudResourceId = cloudResourceId;
	}
	public String getCloudResource() {
		return cloudResource;
	}
	public void setCloudResource(String cloudResource) {
		this.cloudResource = cloudResource;
	}
	public Long getIntegratorResourceId() {
		return integratorResourceId;
	}
	public void setIntegratorResourceId(Long integratorResourceId) {
		this.integratorResourceId = integratorResourceId;
	}
	public String getIntegratorResource() {
		return integratorResource;
	}
	public void setIntegratorResource(String integratorResource) {
		this.integratorResource = integratorResource;
	}
	public Long getClientResourceId() {
		return clientResourceId;
	}
	public void setClientResourceId(Long clientResourceId) {
		this.clientResourceId = clientResourceId;
	}
	public String getClientResource() {
		return clientResource;
	}
	public void setClientResource(String clientResource) {
		this.clientResource = clientResource;
	}
	public String getProjectName() {
		return projectName;
	}
	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}


}
