package com.rite.products.convertrite.po;

public class SaveCloudLookUpSetHeadersResPo {

	private long lookUpSetId;
	private String message;
	private String error;

	public long getLookUpSetId() {
		return lookUpSetId;
	}

	public void setLookUpSetId(long lookUpSetId) {
		this.lookUpSetId = lookUpSetId;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
