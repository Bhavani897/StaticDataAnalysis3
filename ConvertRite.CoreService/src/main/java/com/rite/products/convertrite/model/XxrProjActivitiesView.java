package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name="XXR_PROJ_ACTIVITIES_V")
public class XxrProjActivitiesView {

	
	@Column(name="PROJECT_ID")
	private long projectId;
	@Column(name="TASK_ID")
	private Long taskId;
	@Id
	@Column(name="SEQ")
	private Long seq;
	@Column(name="WBS_ID")
	private long wbsId;
	@Column(name="POD_ID")
	private long podId;
	@Column(name="POD_NAME")
	private String podName;
	@Column(name="TASK_NUM")
	private String taskNum;
	@Column(name="TASK_NAME")
	private String taskName;
	@Column(name="OBJECT_ID")
	private long objectId;
	@Column(name="OBJECT_NAME")
	private String objectCode;
	@Column(name="PARENT_OBJECT_ID")
	private long parentObjectId;
	@Column(name="PARENT_OBJECT_NAME")
	private String parentObjectCode;
	@Column(name="TASK_TYPE")
	private String taskType;
	@Column(name="PRE_REQ_TASK")
	private String preReqTask;
	@Column(name="START_DATE")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date startDate;
	@Column(name="END_DATE")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date endDate;
	@Column(name="WEIGHTAGE")
	private Integer weightage;
	@Column(name="COMPLETE_PERCENTAGE")
	private Integer completePercentage;
	@Column(name="LEGACY_RESOURCE_ID")
	private Long legacyResourceId;
	@Column(name="LEGACY_RESOURCE")
	private String legacyResource;
	@Column(name="TASK_STATUS")
	private String taskStatus;
	@Column(name="DESTINATION_RESOURCE_ID")
	private Long destinationResourceId;
	@Column(name="DESTINATION_RESOURCE")
	private String destinationResource;
	@Column(name="TASK_OWNER_ID")
	private Long taskOwnerId;
	@Column(name="TASK_OWNER")
	private String taskOwner;
	@Column(name="COMPLETION_FLAG")
	private String completionFlag;
	@Column(name="CLOUD_RESOURCE_ID")
	private Long cloudResourceId;
	@Column(name="CLOUD_RESOURCE")
	private String cloudResource;
	@Column(name="INTEGRATOR_RESOURCE_ID")
	private Long integratorResourceId;
	@Column(name="INTEGRATOR_RESOURCE")
	private String integratorResource;
	@Column(name="CLIENT_RESOURCE_ID")
	private Long clientResourceId;
	@Column(name="CLIENT_RESOURCE")
	private String clientResource;
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
	
	
	
}
