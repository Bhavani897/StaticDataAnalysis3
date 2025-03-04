package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrUsers;

public class XxrUserResPo {

	private XxrUsers xxrUser;
	private String message;
	private String error;

	public XxrUsers getXxrUser() {
		return xxrUser;
	}

	public void setXxrUser(XxrUsers xxrUser) {
		this.xxrUser = xxrUser;
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
