package com.rite.products.convertrite.po;

import java.util.List;

public class ProjectsLovPo {

	private long projectId;
	private String projectName;
	private List<ParentObjectLovPo> parentObjects;

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

	public List<ParentObjectLovPo> getParentObjects() {
		return parentObjects;
	}

	public void setParentObjects(List<ParentObjectLovPo> parentObjects) {
		this.parentObjects = parentObjects;
	}

	

}
