package com.rite.products.convertrite.po;

public class LoadMetaDataFromEbsReqPo {

	private String objectCode;
	private String dbLink;
	private String metaDataTableName;
	private String version;
	private String sourceSystem;

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
	}

	public String getDbLink() {
		return dbLink;
	}

	public void setDbLink(String dbLink) {
		this.dbLink = dbLink;
	}

	public String getMetaDataTableName() {
		return metaDataTableName;
	}

	public void setMetaDataTableName(String metaDataTableName) {
		this.metaDataTableName = metaDataTableName;
	}

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
