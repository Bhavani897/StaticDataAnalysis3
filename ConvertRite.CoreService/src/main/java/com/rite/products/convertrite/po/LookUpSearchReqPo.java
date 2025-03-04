package com.rite.products.convertrite.po;

public class LookUpSearchReqPo {

	private String lookupSetName;
	private String operator;
	private Integer pageNo;
	private Integer pageSize;


	public String getLookupSetName() {
		return lookupSetName;
	}

	public void setLookupSetName(String lookupSetName) {
		this.lookupSetName = lookupSetName;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public Integer getPageNo() {
		return pageNo;
	}

	public void setPageNo(Integer pageNo) {
		this.pageNo = pageNo;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

}
