package com.rite.products.convertrite.po;

import org.springframework.data.domain.Sort;

public class XxrSourceFailRecordsReqPo {

	private int pageNo = 0;
	private int pageSize = 50;
	private Sort.Direction sortDirection = Sort.Direction.DESC;
	private String sortBy = "creationDate";

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

	public Sort.Direction getSortDirection() {
		return sortDirection;
	}

	public void setSortDirection(Sort.Direction sortDirection) {
		this.sortDirection = sortDirection;
	}

	public String getSortBy() {
		return sortBy;
	}

	public void setSortBy(String sortBy) {
		this.sortBy = sortBy;
	}

}
