package com.rite.products.convertrite.po;

public class SaveEbsIntegrationResponsePo {

	private Long Id;
	private String error;
	private String message;

	public Long getId() {
		return Id;
	}

	public void setId(Long id) {
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
