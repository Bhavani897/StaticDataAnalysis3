package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_PROJECT_WBS")
public class XxrProjectWbs {

	@Column(name = "PROJECT_ID")
	private long projectId;
	@Id
	@Column(name = "WBS_ID")
	private long wbsId;

	@Column(name = "SEQ")
	private long seq;
	@Column(name="POD_ID")
	private Long podId;
	
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;

	@Column(name = "OBJECT_ID")
	private long objectId;

	@Column(name = "START_DATE")
	private Date startDate;

	@Column(name = "END_DATE")
	private Date endDate;

	@Column(name = "SELECTED")
	private String selectedFlag;
	
	

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public long getProjectId() {
		return projectId;
	}

	public void setProjectId(long projectId) {
		this.projectId = projectId;
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

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
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
