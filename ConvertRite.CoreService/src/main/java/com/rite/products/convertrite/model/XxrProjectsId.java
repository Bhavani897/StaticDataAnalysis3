package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;

public class XxrProjectsId implements Serializable {

	private static final long serialVersionUID = 1L;

	@Column(name = "PROJECT_NAME")
	private String projectName;

	@Column(name = "POD_ID")
	private Long podId;

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	

}
