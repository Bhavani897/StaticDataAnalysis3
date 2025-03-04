package com.rite.products.convertrite.po;

public class LoadDataFromEbsReqPo {
	
	private Long sourceTemplateId;
	private String dbLink;
	//private String objectCode;
	private String version;
	private String sourceSystem;
	private String batchName;
	
	public String getBatchName() {
		return batchName;
	}
	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}
	public Long getSourceTemplateId() {
		return sourceTemplateId;
	}
	public void setSourceTemplateId(Long sourceTemplateId) {
		this.sourceTemplateId = sourceTemplateId;
	}
	public String getDbLink() {
		return dbLink;
	}
	public void setDbLink(String dbLink) {
		this.dbLink = dbLink;
	}

	/*
	 * public String getObjectCode() { return objectCode; } public void
	 * setObjectCode(String objectCode) { this.objectCode = objectCode; }
	 */
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public String getSourceSystem() {
		return sourceSystem;
	}
	public void setSourceSystem(String sourceSystem) {
		this.sourceSystem = sourceSystem;
	}

}
