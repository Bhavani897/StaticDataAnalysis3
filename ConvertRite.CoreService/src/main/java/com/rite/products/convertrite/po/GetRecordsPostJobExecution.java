package com.rite.products.convertrite.po;

public class GetRecordsPostJobExecution {

	private String cloudTemplateName;
	private String status;
	private String type;
	private String batchName;
	private Long pageNo;
	private Long pageSize;
	
	public Long getPageNo() {
		return pageNo;
	}

	public void setPageNo(Long pageNo) {
		this.pageNo = pageNo;
	}

	

	public Long getPageSize() {
		return pageSize;
	}

	public void setPageSize(Long pageSize) {
		this.pageSize = pageSize;
	}

	public String getCloudTemplateName() {
		return cloudTemplateName;
	}

	public void setCloudTemplateName(String cloudTemplateName) {
		this.cloudTemplateName = cloudTemplateName;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getBatchName() {
		return batchName;
	}

	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}

}
