package com.rite.products.convertrite.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;

@Data
@Entity
@Table(name = "CR_USER_HOOKS")
public class CrUserHooks {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "HOOK_ID", columnDefinition = "serial")
	public Long hookId;

	@Column(name = "HOOK_NAME")
	public String hookName;

	@Column(name = "HOOK_CODE")
	public String hookCode;

	@Column(name = "HOOK_TYPE")
	public String hookType;

	@Column(name = "DESCRIPTION")
	public String description;

	@Column(name = "HOOK_TEXT")
	public String hookText;

	@Column(name = "ATTRIBUTE1")
	public String attribute1;

	@Column(name = "ATTRIBUTE2")
	public String attribute2;

	@Column(name = "ATTRIBUTE3")
	public String attribute3;

	@Column(name = "ATTRIBUTE4")
	public String attribute4;

	@Column(name = "ATTRIBUTE5")
	public String attribute5;

	@Column(name = "CREATION_DATE")
	public Date creationDate;
	
	@Column(name = "CREATED_BY")
	public String createdBy;
	
	@Column(name = "LAST_UPDATE_DATE")
	public Date lastUpdateDate;
	
	@Column(name = "LAST_UPDATED_BY")
	public String lastUpdateBy;
}
