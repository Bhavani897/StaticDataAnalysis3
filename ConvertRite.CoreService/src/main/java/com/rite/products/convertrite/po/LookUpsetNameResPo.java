package com.rite.products.convertrite.po;

public class LookUpsetNameResPo {

	private Long lookupSetId;
	private String lookupSetName;

	public LookUpsetNameResPo(Long lookupSetId, String lookupSetName) {
		super();
		this.lookupSetId = lookupSetId;
		this.lookupSetName = lookupSetName;
	}

	public String getLookupSetName() {
		return lookupSetName;
	}

	public void setLookupSetName(String lookupSetName) {
		this.lookupSetName = lookupSetName;
	}

	public Long getLookupSetId() {
		return lookupSetId;
	}

	public void setLookupSetId(Long lookupSetId) {
		this.lookupSetId = lookupSetId;
	}

}
