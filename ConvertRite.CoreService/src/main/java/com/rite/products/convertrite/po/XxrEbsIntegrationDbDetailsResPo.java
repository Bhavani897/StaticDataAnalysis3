package com.rite.products.convertrite.po;

public class XxrEbsIntegrationDbDetailsResPo {

	private Long id;
	private long podId;
	private String podName;
	private long projectId;
	private String projectName;
	private long parentObjectId;
	private String parentObjectCode;
	//private String objectCode;
	//private long objectId;
	private String hostName;
	private String serviceName;
	private String userName;
	private String password;
	private Integer port;
	private String dabaseLink;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
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

	/*
	 * public String getObjectCode() { return objectCode; }
	 * 
	 * public void setObjectCode(String objectCode) { this.objectCode = objectCode;
	 * }
	 * 
	 * public long getObjectId() { return objectId; }
	 * 
	 * public void setObjectId(long objectId) { this.objectId = objectId; }
	 */

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getDabaseLink() {
		return dabaseLink;
	}

	public void setDabaseLink(String dabaseLink) {
		this.dabaseLink = dabaseLink;
	}

}
