package com.rite.products.convertrite.model;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_SRC_TEMP_HDRS")
public class SourceTemplateHeaders implements Serializable {
	private static final long serialVersionUID = 1L;
	@Id
	@Column(name = "TEMPLATE_ID")
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "source_template_header_gen")
	@SequenceGenerator(name = "source_template_header_gen", sequenceName = "XXR_SRC_TEMPLATE_ID_S", allocationSize = 1)
	private long templateId;

	@Column(name = "TEMPLATE_NAME")
	private String templateName;

	@Column(name = "OBJECT_ID")
	private long objectId;

	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;

	@Column(name = "PROJECT_ID")
	private long projectId;

	@Column(name = "NORMALIZE_DATA_FLAG")
	private String normalizeDataFlag;

	@Column(name = "TEMPLATE_CODE")
	private String templateCode;

	@Column(name = "METADATA_TABLE_ID")
	private Long metaDataTableId;

	@Column(name = "STAGING_TABLE_NAME")
	private String stagingTableName;

	@Column(name = "VIEW_NAME")
	private String viewName;

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
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdatedBy;
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@Column(name = "CREATED_BY")
	private String createdBy;

	
	
	public String getNormalizeDataFlag() {
		return normalizeDataFlag;
	}

	public void setNormalizeDataFlag(String normalizeDataFlag) {
		this.normalizeDataFlag = normalizeDataFlag;
	}

	public String getTemplateCode() {
		return templateCode;
	}

	public void setTemplateCode(String templateCode) {
		this.templateCode = templateCode;
	}

	public String getLastUpdatedBy() {
		return lastUpdatedBy;
	}

	public void setLastUpdatedBy(String lastUpdatedBy) {
		this.lastUpdatedBy = lastUpdatedBy;
	}

	public Date getLastUpdatedDate() {
		return lastUpdatedDate;
	}

	public void setLastUpdatedDate(Date lastUpdatedDate) {
		this.lastUpdatedDate = lastUpdatedDate;
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

	public long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(long templateId) {
		this.templateId = templateId;
	}

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
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

	public Long getMetaDataTableId() {
		return metaDataTableId;
	}

	public void setMetaDataTableId(Long metaDataTableId) {
		this.metaDataTableId = metaDataTableId;
	}

	public String getStagingTableName() {
		return stagingTableName;
	}

	public void setStagingTableName(String stagingTableName) {
		this.stagingTableName = stagingTableName;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
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

	public SourceTemplateHeaders(String templateName, long objectId, long parentObjectId, long podId, long projectId,
			Long metaDataTableId, String stagingTableName, String viewName, String bu, String buSpecific,
			String attribute1, String attribute2, String attribute3, String attribute4, String attribute5,
			String lastUpdatedBy, Date lastUpdatedDate, Date creationDate, String createdBy) {
		this.templateName = templateName;
		this.objectId = objectId;
		this.parentObjectId = parentObjectId;
		this.projectId = projectId;
		this.metaDataTableId = metaDataTableId;
		this.stagingTableName = stagingTableName;
		this.viewName = viewName;
		this.attribute1 = attribute1;
		this.attribute2 = attribute2;
		this.attribute3 = attribute3;
		this.attribute4 = attribute4;
		this.attribute5 = attribute5;
		this.lastUpdatedBy = lastUpdatedBy;
		this.lastUpdatedDate = lastUpdatedDate;
		this.creationDate = creationDate;
		this.createdBy = createdBy;
	}

	public SourceTemplateHeaders() {
	}
}
