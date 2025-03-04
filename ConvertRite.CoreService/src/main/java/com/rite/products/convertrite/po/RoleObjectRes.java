package com.rite.products.convertrite.po;

public class RoleObjectRes {

	private long roleId;
	private String roleName;
	private long parentObjectId;
	private String parentObjectCode;
	//private long podId;
	//private String podName;
	private long projectId;
	private String projectName;
	private String enableFlag;

	public String getEnableFlag() {
		return enableFlag;
	}

	public void setEnableFlag(String enableFlag) {
		this.enableFlag = enableFlag;
	}

	public long getRoleId() {
		return roleId;
	}

	public void setRoleId(long roleId) {
		this.roleId = roleId;
	}

	public String getRoleName() {
		return roleName;
	}

	public void setRoleName(String roleName) {
		this.roleName = roleName;
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

	/*
	 * public long getPodId() { return podId; }
	 * 
	 * public void setPodId(long podId) { this.podId = podId; }
	 */

	/*
	 * public String getPodName() { return podName; }
	 * 
	 * public void setPodName(String podName) { this.podName = podName; }
	 */

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

}
