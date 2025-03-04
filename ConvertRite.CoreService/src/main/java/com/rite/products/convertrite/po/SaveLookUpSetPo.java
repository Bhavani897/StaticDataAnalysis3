package com.rite.products.convertrite.po;

public class SaveLookUpSetPo {

	private Long lookUpSetId;
	private String lookUpSetName;
	private String description;
	private String relatedTo;
	private String lookupFlag;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;

	/*
	 * private String lastUpdatedBy; private String createdBy;
	 */

	/*
	 * public String getLastUpdatedBy() { return lastUpdatedBy; }
	 * 
	 * public void setLastUpdatedBy(String lastUpdatedBy) { this.lastUpdatedBy =
	 * lastUpdatedBy; }
	 * 
	 * public String getCreatedBy() { return createdBy; }
	 * 
	 * public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
	 */

	
	public String getAttribute1() {
		return attribute1;
	}

	public String getLookupFlag() {
		return lookupFlag;
	}

	public void setLookupFlag(String lookupFlag) {
		this.lookupFlag = lookupFlag;
	}

	public void setAttribute1(String attribute1) {
		this.attribute1 = attribute1;
	}

	public String getAttribute2() {
		return attribute2;
	}

	public void setAttribute2(String attribute2) {
		this.attribute2 = attribute2;
	}

	public String getAttribute3() {
		return attribute3;
	}

	public void setAttribute3(String attribute3) {
		this.attribute3 = attribute3;
	}

	public String getAttribute4() {
		return attribute4;
	}

	public void setAttribute4(String attribute4) {
		this.attribute4 = attribute4;
	}

	public String getAttribute5() {
		return attribute5;
	}

	public void setAttribute5(String attribute5) {
		this.attribute5 = attribute5;
	}

	public Long getLookUpSetId() {
		return lookUpSetId;
	}

	public void setLookUpSetId(Long lookUpSetId) {
		this.lookUpSetId = lookUpSetId;
	}

	public String getLookUpSetName() {
		return lookUpSetName;
	}

	public void setLookUpSetName(String lookUpSetName) {
		this.lookUpSetName = lookUpSetName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getRelatedTo() {
		return relatedTo;
	}

	public void setRelatedTo(String relatedTo) {
		this.relatedTo = relatedTo;
	}

}
