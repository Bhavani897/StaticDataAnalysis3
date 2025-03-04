package com.rite.products.convertrite.po;

public class ObjectCodeLovPo {
	
	

	private long objectId;
	private String objectCode;

	
	
	public ObjectCodeLovPo(long objectId, String objectCode) {
		super();
		this.objectId = objectId;
		this.objectCode = objectCode;
	}

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
	}

}
