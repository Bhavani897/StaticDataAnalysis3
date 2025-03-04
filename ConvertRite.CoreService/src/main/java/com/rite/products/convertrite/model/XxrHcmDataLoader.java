package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name = "XXR_HCM_DATA_LOADER")
public class XxrHcmDataLoader {

	@Id
	@Column(name = "ID")
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "hcm_data_generator")
	@SequenceGenerator(name = "hcm_data_generator", sequenceName = "xxr_hcm_data_loader_id_s", allocationSize = 1)
	private long id;
	@Column(name = "CLOUD_TEMPLATE_ID")
	private long cloudTemplateId;
	@Column(name = "DOCUMENT_TITLE")
	private String documentTitle;
	@Column(name = "DOCUMENT_AUTHOR")
	private String documentAuthor;
	@Column(name = "DOCUMENT_SECURITY_GROUP")
	private String documentSecurityGroup;
	@Column(name = "DOCUMENT_ACCOUNT")
	private String documentAccount;
	@Column(name = "CONTENT_ID")
	private String contentId;
	@Column(name = "PROCESS_ID")
	private String processId;
	@Column(name="POD_ID")
	private Long podId;
	@Column(name="PROJECT_ID")
	private Long projectId;
	@Column(name="PARENT_OBJECT_ID")
	private Long parentObjectId;
	@Column(name="XXR_BATCH_NAME")
	private String xxrBatchName;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy;

	
	
	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getXxrBatchName() {
		return xxrBatchName;
	}

	public void setXxrBatchName(String xxrBatchName) {
		this.xxrBatchName = xxrBatchName;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public Date getLastUpdatedDate() {
		return lastUpdatedDate;
	}

	public void setLastUpdatedDate(Date lastUpdatedDate) {
		this.lastUpdatedDate = lastUpdatedDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getLastUpdateBy() {
		return lastUpdateBy;
	}

	public void setLastUpdateBy(String lastUpdateBy) {
		this.lastUpdateBy = lastUpdateBy;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getCloudTemplateId() {
		return cloudTemplateId;
	}

	public void setCloudTemplateId(long cloudTemplateId) {
		this.cloudTemplateId = cloudTemplateId;
	}

	public String getDocumentTitle() {
		return documentTitle;
	}

	public void setDocumentTitle(String documentTitle) {
		this.documentTitle = documentTitle;
	}

	public String getDocumentAuthor() {
		return documentAuthor;
	}

	public void setDocumentAuthor(String documentAuthor) {
		this.documentAuthor = documentAuthor;
	}

	public String getDocumentSecurityGroup() {
		return documentSecurityGroup;
	}

	public void setDocumentSecurityGroup(String documentSecurityGroup) {
		this.documentSecurityGroup = documentSecurityGroup;
	}

	public String getDocumentAccount() {
		return documentAccount;
	}

	public void setDocumentAccount(String documentAccount) {
		this.documentAccount = documentAccount;
	}

	public String getContentId() {
		return contentId;
	}

	public void setContentId(String contentId) {
		this.contentId = contentId;
	}

	public String getProcessId() {
		return processId;
	}

	public void setProcessId(String processId) {
		this.processId = processId;
	}

}
