package com.rite.products.convertrite.po;

public class ProjectsPo {

	private final Long projectId;
	private final String projectName;
	public ProjectsPo(Long projectId, String projectName) {
		super();
		this.projectId = projectId;
		this.projectName = projectName;
	}
	public Long getProjectId() {
		return projectId;
	}

	/*
	 * public void setProjectId(Integer projectId) { this.projectId = projectId; }
	 */
	public String getProjectName() {
		return projectName;
	}
	/*
	 * public void setProjectName(Integer projectName) { this.projectName =
	 * projectName; }
	 */
	
	
}
