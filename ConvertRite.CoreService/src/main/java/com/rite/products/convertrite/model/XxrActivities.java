package com.rite.products.convertrite.model;

import java.io.Serializable;
import java.sql.Array;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.*;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.rite.products.convertrite.po.ActivitiesPo;

@Entity
@Table(name = "XXR_PROJ_ACTIVITIES")
@NamedStoredProcedureQueries({
		@NamedStoredProcedureQuery(name = "in_and_out_test",
				procedureName = "XXR_PROJECT_SAVE_PKG.XXR_LOAD_TASK_PRC",
				parameters = {
						@StoredProcedureParameter(mode = ParameterMode.IN, name = "p_project_id", type = Integer.class),
						@StoredProcedureParameter(mode = ParameterMode.IN, name = "p_pod_id", type = Integer.class),
						@StoredProcedureParameter(mode = ParameterMode.IN, name = "p_user_id", type = String.class),

						@StoredProcedureParameter(mode = ParameterMode.OUT, name = "p_msg", type = String.class)
				}),@NamedStoredProcedureQuery(name = "in_and_out_test_1",
		procedureName = "xxr_project_save_pkg.xxr_proj_activities_prc",
		parameters = {
				@StoredProcedureParameter(mode = ParameterMode.IN, name = "p_xxr_proj_activities", type = Object.class),
				@StoredProcedureParameter(mode = ParameterMode.IN, name = "p_user_id", type = String.class),

				@StoredProcedureParameter(mode = ParameterMode.OUT, name = "p_msg", type = String.class)
		})
})
public class XxrActivities implements Serializable {

	@Column(name = "PROJECT_ID")
	private long projectId;
	@Column(name = "TASK_ID")
	private Long taskId;
	@Id
	@Column(name = "SEQ")
	private long seq;
	@Column(name="POD_ID")
	private Long podId;
	@Column(name = "WBS_ID")
	private long wbsId;
	@Column(name = "TASK_NUM")
	private String taskNum;
	@Column(name = "TASK_NAME")
	private String taskName;
	@Column(name = "OBJECT_ID")
	private long objectId;
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;
	@Column(name = "TASK_TYPE")
	private String taskType;
	@Column(name = "PRE_REQ_TASK")
	private String preReqTask;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "START_DATE")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "END_DATE")
	private Date endDate;
	@Column(name = "WEIGHTAGE")
	private Integer weightage;
	/*
	 * @Column(name = "COMPLETED_FLAG") private String taskStatus;
	 */
	@Column(name = "COMPLETE_PERCENTAGE")
	private Integer completePercentage;
	@Column(name = "LEGACY_RESOURCE_ID")
	private Long legacyResourceId;
	@Column(name = "TASK_STATUS")
	private String taskStatus;
	@Column(name = "DESTINATION_RESOURCE_ID")
	private Long destinationResourceId;
	@Column(name = "TASK_OWNER_ID")
	private Long taskOwnerId;
	@Column(name = "COMPLETION_FLAG")
	private String completionFlag;
	@Column(name = "CLOUD_RESOURCE_ID")
	private Long cloudResourceId;
	@Column(name = "INTEGRATOR_RESOURCE_ID")
	private Long integratorResourceId;
	@Column(name = "CLIENT_RESOURCE_ID")
	private Long clientResourceId;

	
	
	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public long getWbsId() {
		return wbsId;
	}

	public void setWbsId(long wbsId) {
		this.wbsId = wbsId;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
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

	@Override
	public String toString() {
		return "XxrActivities [projectId=" + projectId + ", taskId=" + taskId + ", seq=" + seq + ", podId=" + podId
				+ ", wbsId=" + wbsId + ", taskNum=" + taskNum + ", taskName=" + taskName + ", objectId=" + objectId
				+ ", parentObjectId=" + parentObjectId + ", taskType=" + taskType + ", preReqTask=" + preReqTask
				+ ", startDate=" + startDate + ", endDate=" + endDate + ", weightage=" + weightage
				+ ", completePercentage=" + completePercentage + ", legacyResourceId=" + legacyResourceId
				+ ", taskStatus=" + taskStatus + ", destinationResourceId=" + destinationResourceId + ", taskOwnerId="
				+ taskOwnerId + ", completionFlag=" + completionFlag + ", cloudResourceId=" + cloudResourceId
				+ ", integratorResourceId=" + integratorResourceId + ", clientResourceId=" + clientResourceId + "]";
	}
	

}
