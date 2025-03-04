package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;

public class XxrRoleObjectLinksId implements Serializable {

	private static final long serialVersionUID = 1L;
	
	@Column(name="ROLE_ID")
	private long roleId;
	@Column(name="OBJECT_ID")
	private long parentObjectId;
	@Column(name="PROJECT_ID")
	private long projectId;
	/*
	 * @Column(name="POD_ID") private long podId;
	 */
	
	public long getProjectId() {
		return projectId;
	}

	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	/*
	 * public long getPodId() { return podId; }
	 * 
	 * public void setPodId(long podId) { this.podId = podId; }
	 */

	public long getRoleId() {
		return roleId;
	}

	public void setRoleId(long roleId) {
		this.roleId = roleId;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	

}
