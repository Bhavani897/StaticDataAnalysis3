package com.rite.products.convertrite.po;

public class ParentObjectCodeResPo {
	
	private long parentObjectId;
	private String parentObjectCode;
	
	public ParentObjectCodeResPo(long parentObjectId, String parentObjectCode) {
		super();
		this.parentObjectId = parentObjectId;
		this.parentObjectCode = parentObjectCode;
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
	
	

}
