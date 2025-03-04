package com.rite.products.convertrite.po;

import java.sql.Date;

public class XxrProjectWbsResPo {

	private long projectId;
	private String projectName;
	private long wbsId;
	private Long podId;
	private String podName;
	private long seq;
	private long parentObjectId;
	private String parentObjectCode;
	private long objectId;
	private String objectCode;
	private Date startDate;
	private Date endDate;
	private String selectedFlag;

	

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
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

	public long getWbsId() {
		return wbsId;
	}

	public void setWbsId(long wbsId) {
		this.wbsId = wbsId;
	}

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
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

	public String getSelectedFlag() {
		return selectedFlag;
	}

	public void setSelectedFlag(String selectedFlag) {
		this.selectedFlag = selectedFlag;
	}

}
