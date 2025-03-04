package com.rite.products.convertrite.po;

public class ReconcileDetailsReqPo {

	private Long cldTemplateId;
	private String batchName;
	private int pageNo;
	private int pageSize;

	public Long getCldTemplateId() {
		return cldTemplateId;
	}

	public void setCldTemplateId(Long cldTemplateId) {
		this.cldTemplateId = cldTemplateId;
	}

	public String getBatchName() {
		return batchName;
	}

	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

}
