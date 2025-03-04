package com.rite.products.convertrite.po;

public class SaveReconcileConfigReqPo {
	private Long id;
	private Long objectId;
	private String objectCode;
	private String version;
	private String sqlViewQuery;
	private String whereClause;

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
		this.objectId = objectId;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
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

	public String getSqlViewQuery() {
		return sqlViewQuery;
	}

	public void setSqlViewQuery(String sqlViewQuery) {
		this.sqlViewQuery = sqlViewQuery;
	}

	public String getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(String whereClause) {
		this.whereClause = whereClause;
	}

}
