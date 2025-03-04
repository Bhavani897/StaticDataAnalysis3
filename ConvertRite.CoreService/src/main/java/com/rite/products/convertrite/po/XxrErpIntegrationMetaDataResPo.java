package com.rite.products.convertrite.po;

public class XxrErpIntegrationMetaDataResPo {

	private Long id;
	/*
	 * private String podName; private long podId; private String projectName;
	 * private long projectId;
	 */
	private String parentObjectCode;
	private long parentObjectId;
	private String objectCode;
	private long objectId;
	private String documentAccount;
	private String jobName;
	private String interfaceDetails;
	private String documentSecurityGroup;
	
	

	public String getDocumentSecurityGroup() {
		return documentSecurityGroup;
	}

	public void setDocumentSecurityGroup(String documentSecurityGroup) {
		this.documentSecurityGroup = documentSecurityGroup;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	/*
	 * public String getPodName() { return podName; }
	 * 
	 * public void setPodName(String podName) { this.podName = podName; }
	 * 
	 * public long getPodId() { return podId; }
	 * 
	 * public void setPodId(long podId) { this.podId = podId; }
	 * 
	 * public String getProjectName() { return projectName; }
	 * 
	 * public void setProjectName(String projectName) { this.projectName =
	 * projectName; }
	 * 
	 * public long getProjectId() { return projectId; }
	 * 
	 * public void setProjectId(long projectId) { this.projectId = projectId; }
	 */
	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
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
