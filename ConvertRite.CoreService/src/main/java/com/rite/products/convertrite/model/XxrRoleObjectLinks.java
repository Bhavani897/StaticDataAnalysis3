package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;

@Entity
@Table(name="xxr_role_obj_links")
@IdClass(XxrRoleObjectLinksId.class)
public class XxrRoleObjectLinks {

	@Id
	@Column(name="ROLE_ID")
	private long roleId;
	@Id
	@Column(name="PROJECT_ID")
	private long projectId;
	/*
	 * @Id
	 * 
	 * @Column(name="POD_ID") private long podId;
	 */
	@Id
	@Column(name="OBJECT_ID")
	private long parentObjectId;
	@Column(name="ENABLE_FLAG")
	private String enableFlag;
	public long getRoleId() {
		return roleId;
	}
	public void setRoleId(long roleId) {
		this.roleId = roleId;
	}
	public long getProjectId() {
		return projectId;
	}
	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}
	/*
	 * public long getPodId() { return podId; } public void setPodId(long podId) {
	 * this.podId = podId; }
	 */
	
	public long getParentObjectId() {
		return parentObjectId;
	}
	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}
	public String getEnableFlag() {
		return enableFlag;
	}
	public void setEnableFlag(String enableFlag) {
		this.enableFlag = enableFlag;
	}
	
	
}
