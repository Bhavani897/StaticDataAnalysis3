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
@Table(name = "XXR_ERP_INTEGRATIONS_METADATA")
public class XxrErpIntegrationMetaData {

	@Id
	@Column(name = "ID")
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "erp_metadata_generator")
	@SequenceGenerator(name = "erp_metadata_generator", sequenceName = "xxr_erp_integrations_metadata_id_s", allocationSize = 1)
	private Long id;
	/*
	 * @Column(name = "POD_ID") private long podId;
	 * 
	 * @Column(name = "PROJECT_ID") private long projectId;
	 */
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;
	@Column(name = "OBJECT_ID")
	private long objectId;
	@Column(name = "DOCUMENT_ACCOUNT")
	private String documentAccount;
	@Column(name = "JOB_NAME")
	private String jobName;
	@Column(name = "INTERFACE_DETAILS")
	private String interfaceDetails;
	@Column(name="DOCUMENT_SECURITY_GROUP")
	private String documentSecurityGroup;
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

	public String getDocumentSecurityGroup() {
		return documentSecurityGroup;
	}

	public void setDocumentSecurityGroup(String documentSecurityGroup) {
		this.documentSecurityGroup = documentSecurityGroup;
	}

	/*
	 * public long getPodId() { return podId; }
	 * 
	 * public void setPodId(long podId) { this.podId = podId; }
	 * 
	 * public long getProjectId() { return projectId; }
	 * 
	 * public void setProjectId(long projectId) { this.projectId = projectId; }
	 */

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
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

	public String getInterfaceDetails() {
		return interfaceDetails;
	}

	public void setInterfaceDetails(String interfaceDetails) {
		this.interfaceDetails = interfaceDetails;
	}

}
