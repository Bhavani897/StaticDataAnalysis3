package com.rite.products.convertrite.po;

import java.util.List;

public class ParentObjectLovPo {

	private long parentObjectId;
	private String parentObjectCode;
	private List<ObjectCodeLovPo> objectCodes;
	

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

	public List<ObjectCodeLovPo> getObjectCodes() {
		return objectCodes;
	}

	public void setObjectCodes(List<ObjectCodeLovPo> objectCodes) {
		this.objectCodes = objectCodes;
	}
}
