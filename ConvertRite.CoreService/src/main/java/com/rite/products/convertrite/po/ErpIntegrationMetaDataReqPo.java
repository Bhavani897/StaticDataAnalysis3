package com.rite.products.convertrite.po;

public class ErpIntegrationMetaDataReqPo {

	/*
	 * private Long podId; private Long projectId;
	 * 
	 */	private Long parentObjectId;
	private Long objectId;
	private String documentAccount;
	private String jobName;
	private String interfaceDetails;
	private String documentSecurityGroup;
	private Long id;
	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getDocumentSecurityGroup() {
		return documentSecurityGroup;
	}

	public void setDocumentSecurityGroup(String documentSecurityGroup) {
		this.documentSecurityGroup = documentSecurityGroup;
	}

	/*
	 * public Long getPodId() { return podId; }
	 * 
	 * public void setPodId(Long podId) { this.podId = podId; }
	 * 
	 * public Long getProjectId() { return projectId; }
	 * 
	 * public void setProjectId(Long projectId) { this.projectId = projectId; }
	 */

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
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
