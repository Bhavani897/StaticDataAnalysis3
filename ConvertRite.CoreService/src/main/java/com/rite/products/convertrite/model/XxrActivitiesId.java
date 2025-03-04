package com.rite.products.convertrite.model;

import java.io.Serializable;

public class XxrActivitiesId implements Serializable {

	private static final long serialVersionUID = 1L;

	private Double taskNumber;
	private Long projectId;

	public Double getTaskNumber() {
		return taskNumber;
	}

	public void setTaskNumber(Double taskNumber) {
		this.taskNumber = taskNumber;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

}
