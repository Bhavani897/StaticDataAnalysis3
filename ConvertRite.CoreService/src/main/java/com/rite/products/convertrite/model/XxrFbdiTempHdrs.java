package com.rite.products.convertrite.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_FBDI_TEMP_HDRS")
public class XxrFbdiTempHdrs {

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "fbdi_temp_hdrs_gen")
	@SequenceGenerator(name = "fbdi_temp_hdrs_gen", sequenceName = "xxr_fbdi_template_id_s", allocationSize = 1)
	@Column(name = "FBDI_TEMPLATE_ID")
	private long fbdiTemplateId;
	@Column(name = "FBDI_TEMPLATE_NAME")
	private String fbdiTemplateName;
	@Column(name = "POD_ID")
	private long podId;
	@Column(name = "PROJECT_ID")
	private long projectId;
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;
	@Column(name = "OBJECT_ID")
	private long objectId;
	@Column(name = "VERSION ")
	private String version;
	@Column(name = "SHEET_NAME")
	private String sheetName;
	@Column(name = "API")
	private String api;
	@Lob
	@Column(name = "CTL_FILE_BLOB")
	private byte[] ctlFileBlob;
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdatedBy;
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "ATTRIBUTE1")
	private String attribute1;
	@Column(name = "ATTRIBUTE2")
	private String attribute2;
	@Column(name = "ATTRIBUTE3")
	private String attribute3;
	@Column(name = "ATTRIBUTE4")
	private String attribute4;
	@Column(name = "ATTRIBUTE5")
	private String attribute5;

	public Date getLastUpdatedDate() {
		return lastUpdatedDate;
	}

	public void setLastUpdatedDate(Date lastUpdatedDate) {
		this.lastUpdatedDate = lastUpdatedDate;
	}

	public String getLastUpdatedBy() {
		return lastUpdatedBy;
	}

	public void setLastUpdatedBy(String lastUpdatedBy) {
		this.lastUpdatedBy = lastUpdatedBy;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getAttribute1() {
		return attribute1;
	}

	public void setAttribute1(String attribute1) {
		this.attribute1 = attribute1;
	}

	public String getAttribute2() {
		return attribute2;
	}

	public void setAttribute2(String attribute2) {
		this.attribute2 = attribute2;
	}

	public String getAttribute3() {
		return attribute3;
	}

	public void setAttribute3(String attribute3) {
		this.attribute3 = attribute3;
	}

	public String getAttribute4() {
		return attribute4;
	}

	public void setAttribute4(String attribute4) {
		this.attribute4 = attribute4;
	}

	public String getAttribute5() {
		return attribute5;
	}

	public void setAttribute5(String attribute5) {
		this.attribute5 = attribute5;
	}

	public byte[] getCtlFileBlob() {
		return ctlFileBlob;
	}

	public void setCtlFileBlob(byte[] ctlFileBlob) {
		this.ctlFileBlob = ctlFileBlob;
	}

	public String getApi() {
		return api;
	}

	public void setApi(String api) {
		this.api = api;
	}

	public long getProjectId() {
		return projectId;
	}

	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
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

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

}
