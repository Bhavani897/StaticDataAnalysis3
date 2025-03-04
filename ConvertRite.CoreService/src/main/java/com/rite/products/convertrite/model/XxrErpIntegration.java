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
@Table(name = "XXR_ERP_INTEGRATIONS")
public class XxrErpIntegration {
	@Id
	@Column(name = "ID")
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "erp_generator")
	@SequenceGenerator(name = "erp_generator", sequenceName = "xxr_erp_integrations_id_s", allocationSize = 1)
	private Long id;
	@Column(name = "FILE_NAME")
	private String fileName;
	@Column(name = "CONTENT_TYPE")
	private String contentType;
	@Column(name = "DOCUMENT_TITLE")
	private String documentTitle;
	@Column(name = "DOCUMENT_AUTHOR")
	private String documentAuthor;
	@Column(name = "DOCUMENT_SECURITY_GROUP")
	private String documentSecurityGroup;
	@Column(name = "DOCUMENT_ACCOUNT")
	private String documentAccount;
	@Column(name = "JOB_NAME")
	private String jobName;
	@Column(name = "PARAMETER_LIST")
	private String parameterList;
	@Column(name = "INTERFACE_DETAILS")
	private String interfaceDetails;
	@Column(name = "RESULT")
	private Long result;
	@Column(name = "CLOUD_TEMPLATE_ID")
	private Long cloudTemplateId;
	@Column(name = "JOB_STATUS")
	private String jobStatus;
	@Column(name="XXR_BATCH_NAME")	private String xxrBatchName;
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

	public String getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
	}

	public Long getCloudTemplateId() {
		return cloudTemplateId;
	}

	public void setCloudTemplateId(Long cloudTemplateId) {
		this.cloudTemplateId = cloudTemplateId;
	}

	public Long getResult() {
		return result;
	}

	public void setResult(Long result) {
		this.result = result;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
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

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getParameterList() {
		return parameterList;
	}

	public void setParameterList(String parameterList) {
		this.parameterList = parameterList;
	}

	public String getInterfaceDetails() {
		return interfaceDetails;
	}

	public void setInterfaceDetails(String interfaceDetails) {
		this.interfaceDetails = interfaceDetails;
	}

}
