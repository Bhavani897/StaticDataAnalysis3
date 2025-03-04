package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;

public class XxrUserRolesId implements Serializable {

	private static final long serialVersionUID = 1L;

	@Column(name = "USER_ID")
	private Long userId;
	@Column(name = "ROLE_ID")
	private Long roleId;

	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public Long getRoleId() {
		return roleId;
	}

	public void setRoleId(Long roleId) {
		this.roleId = roleId;
	}

}
