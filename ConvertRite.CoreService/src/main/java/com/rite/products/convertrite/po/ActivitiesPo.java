package com.rite.products.convertrite.po;

import java.io.Serializable;
import java.sql.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class ActivitiesPo implements
		Serializable {

	private Long projectId;
	private Long taskId;
	private Long seq;
	private Long wbsId;
	private Long podId;
	private String taskNum;
	private String taskName;
	private Long objectId;
	private Long parentObjectId;
	private String taskType;
	private String preReqTask;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date endDate;
	private Integer weightage;
	private Integer completePercentage;
	private Long legacyResourceId;
	private String taskStatus;
	private Long destinationResourceId;
	private Long taskOwnerId;
	private String completionFlag;
	private Long cloudResourceId;
	private Long integratorResourceId;
	private Long clientResourceId;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;

	
	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public Long getWbsId() {
		return wbsId;
	}

	public void setWbsId(Long wbsId) {
		this.wbsId = wbsId;
	}

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public Long getSeq() {
		return seq;
	}

	public void setSeq(Long seq) {
		this.seq = seq;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public Long getTaskId() {
		return taskId;
	}

	public void setTaskId(Long taskId) {
		this.taskId = taskId;
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

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
		this.objectId = objectId;
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

	public Long getTaskOwnerId() {
		return taskOwnerId;
	}

	public void setTaskOwnerId(Long taskOwnerId) {
		this.taskOwnerId = taskOwnerId;
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

	public Long getIntegratorResourceId() {
		return integratorResourceId;
	}

	public void setIntegratorResourceId(Long integratorResourceId) {
		this.integratorResourceId = integratorResourceId;
	}

	public Long getClientResourceId() {
		return clientResourceId;
	}

	public void setClientResourceId(Long clientResourceId) {
		this.clientResourceId = clientResourceId;
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
