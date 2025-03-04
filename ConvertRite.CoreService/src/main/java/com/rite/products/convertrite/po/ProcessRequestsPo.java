package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrProcessRequests;

public class ProcessRequestsPo {
	
	private XxrProcessRequests xxrProcessRequests;
	
	private Integer successRecords;
	
	private Integer failureRecords;
	
	private String parentObjectCode;

	

	public ProcessRequestsPo(XxrProcessRequests xxrProcessRequests, Integer successRecords, Integer failureRecords) {
		super();
		this.xxrProcessRequests = xxrProcessRequests;
		this.successRecords = successRecords;
		this.failureRecords = failureRecords;
		//this.parentObjectCode = parentObjectCode;
	}

	public XxrProcessRequests getXxrProcessRequests() {
		return xxrProcessRequests;
	}

	public void setXxrProcessRequests(XxrProcessRequests xxrProcessRequests) {
		this.xxrProcessRequests = xxrProcessRequests;
	}

	public Integer getSuccessRecords() {
		return successRecords;
	}

	public void setSuccessRecords(Integer successRecords) {
		this.successRecords = successRecords;
	}

	public Integer getFailureRecords() {
		return failureRecords;
	}

	public void setFailureRecords(Integer failureRecords) {
		this.failureRecords = failureRecords;
	}

	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}
	
	
	

}
