package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrUserHooksDetails;

public class SaveUserHooksResPo {

	private XxrUserHooksDetails XxrUserHooksDetails;
	private String message;
	private String error;

	public XxrUserHooksDetails getXxrUserHooksDetails() {
		return XxrUserHooksDetails;
	}

	public void setXxrUserHooksDetails(XxrUserHooksDetails xxrUserHooksDetails) {
		XxrUserHooksDetails = xxrUserHooksDetails;
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
