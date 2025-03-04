package com.rite.products.convertrite.po;

public class SaveDateConfigurationResPo {

	private long Id;
	private String error;
	private String message;

	public long getId() {
		return Id;
	}

	public void setId(long id) {
		Id = id;
	}

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

}
