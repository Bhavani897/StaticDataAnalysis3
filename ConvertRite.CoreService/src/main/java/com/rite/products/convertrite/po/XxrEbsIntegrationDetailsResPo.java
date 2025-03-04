package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrEbsIntegrationDbDetails;

public class XxrEbsIntegrationDetailsResPo {
	
	
	private XxrEbsIntegrationDbDetails xxrEbsIntegrationDbDetails;
	private String podName;
	private String projectName;
	private String parentObjectCode;
	
	public XxrEbsIntegrationDetailsResPo(XxrEbsIntegrationDbDetails xxrEbsIntegrationDbDetails, String podName,
			String projectName, String parentObjectCode) {
		super();
		this.xxrEbsIntegrationDbDetails = xxrEbsIntegrationDbDetails;
		this.podName = podName;
		this.projectName = projectName;
		this.parentObjectCode = parentObjectCode;
	}
	public XxrEbsIntegrationDbDetails getXxrEbsIntegrationDbDetails() {
		return xxrEbsIntegrationDbDetails;
	}
	public void setXxrEbsIntegrationDbDetails(XxrEbsIntegrationDbDetails xxrEbsIntegrationDbDetails) {
		this.xxrEbsIntegrationDbDetails = xxrEbsIntegrationDbDetails;
	}
	public String getPodName() {
		return podName;
	}
	public void setPodName(String podName) {
		this.podName = podName;
	}
	public String getProjectName() {
		return projectName;
	}
	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}
	public String getParentObjectCode() {
		return parentObjectCode;
	}
	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}
	
	

}
