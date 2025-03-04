package com.rite.products.convertrite.po;

public class SaveEbsViewReqpo {

	private String ebsViewSql;
	private String sourceSystem;
	private String objectCode;
	private String version;

	public String getEbsViewSql() {
		return ebsViewSql;
	}

	public void setEbsViewSql(String ebsViewSql) {
		this.ebsViewSql = ebsViewSql;
	}

	public String getSourceSystem() {
		return sourceSystem;
	}

	public void setSourceSystem(String sourceSystem) {
		this.sourceSystem = sourceSystem;
	}

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

}
