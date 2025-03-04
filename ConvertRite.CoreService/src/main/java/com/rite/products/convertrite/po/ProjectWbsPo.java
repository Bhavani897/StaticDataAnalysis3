package com.rite.products.convertrite.po;

public class ProjectWbsPo {

	private long projectId;
	private long objectId;
	private long parentObjectId;
	private String selectedFlag;
	private long podId;
	
	
	public long getPodId() {
		return podId;
	}

	public void setPodId(long podId) {
		this.podId = podId;
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

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}

	public String getSelectedFlag() {
		return selectedFlag;
	}

	public void setSelectedFlag(String selectedFlag) {
		this.selectedFlag = selectedFlag;
	}

}
