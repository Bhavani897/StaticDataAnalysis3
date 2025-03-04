package com.rite.products.convertrite.po;

public class LoadDataFromEbsResPo {

	private String message;
	private String error;
	private String stagingTableName;
	private Integer loadedRecCount;

	
	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getStagingTableName() {
		return stagingTableName;
	}

	public void setStagingTableName(String stagingTableName) {
		this.stagingTableName = stagingTableName;
	}

	public Integer getLoadedRecCount() {
		return loadedRecCount;
	}

	public void setLoadedRecCount(Integer loadedRecCount) {
		this.loadedRecCount = loadedRecCount;
	}

}
