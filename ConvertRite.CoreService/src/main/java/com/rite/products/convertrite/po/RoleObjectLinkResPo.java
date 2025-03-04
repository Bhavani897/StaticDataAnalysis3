package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrRoleObjectLinks;

public class RoleObjectLinkResPo {

	private List<XxrRoleObjectLinks> roleObjectLink;
	private String message;
	private String error;

	public List<XxrRoleObjectLinks> getRoleObjectLink() {
		return roleObjectLink;
	}

	public void setRoleObjectLink(List<XxrRoleObjectLinks> roleObjectLink) {
		this.roleObjectLink = roleObjectLink;
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
