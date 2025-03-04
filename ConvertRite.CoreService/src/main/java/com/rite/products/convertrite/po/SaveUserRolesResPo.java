package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrUserRoles;

public class SaveUserRolesResPo {

	private List<XxrUserRoles> userRoles;
	private String message;
	private String error;

	public List<XxrUserRoles> getUserRoles() {
		return userRoles;
	}

	public void setUserRoles(List<XxrUserRoles> userRoles) {
		this.userRoles = userRoles;
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
