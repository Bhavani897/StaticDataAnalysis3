package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_FBDI_TEMP_HDRS_V")
public class FbdiTempHdrsView {
	@Id
	@Column(name = "FBDI_TEMPLATE_ID")
	private long fbdiTemplateId;
	@Column(name = "FBDI_TEMPLATE_NAME")
	private String fbdiTemplateName;
	@Column(name = "POD_ID")
	private long podId;
	@Column(name = "POD_NAME")
	private String podName;
	@Column(name = "PROJECT_ID")
	private long projectId;
	@Column(name = "PROJECT_NAME")
	private String projectName;
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;
	@Column(name = "PARENT_OBJECT_CODE")
	private String parentObjectCode;
	@Column(name = "OBJECT_ID")
	private long objectId;
	@Column(name = "OBJECT_CODE")
	private String objectCode;
	@Column(name = "VERSION")
	private String version;
	@Column(name = "SHEET_NAME")
	private String sheetName;
	@Column(name = "API")
	private String api;
	@Column(name="ROLE_ID")
	private Long roleId;
	
	

	public Long getRoleId() {
		return roleId;
	}

	public void setRoleId(Long roleId) {
		this.roleId = roleId;
	}

	public long getFbdiTemplateId() {
		return fbdiTemplateId;
	}

	public void setFbdiTemplateId(long fbdiTemplateId) {
		this.fbdiTemplateId = fbdiTemplateId;
	}

	public String getFbdiTemplateName() {
		return fbdiTemplateName;
	}

	public void setFbdiTemplateName(String fbdiTemplateName) {
		this.fbdiTemplateName = fbdiTemplateName;
	}

	public long getPodId() {
		return podId;
	}

	public void setPodId(long podId) {
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

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public String getApi() {
		return api;
	}

	public void setApi(String api) {
		this.api = api;
	}

}
