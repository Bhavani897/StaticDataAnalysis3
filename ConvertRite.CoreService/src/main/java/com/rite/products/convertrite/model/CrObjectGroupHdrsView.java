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
@Table(name = "CR_OBJECT_GROUP_HDRS_V")
public class CrObjectGroupHdrsView {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "GROUP_ID")
	private Long groupId;
	
	@Column(name = "GROUP_NAME")
	private String groupName;
	
	@Column(name = "GROUP_CODE")
	private String groupCode;
	
	@Column(name = "PROJECT_ID")
	private Long projectId;
	
	@Column(name = "PROJECT_NAME")
	private String projectName;
	
	@Column(name = "PARENT_OBJECT_ID")
	private Long parentObjectId;
	
	@Column(name = "PARENT_OBJECT_NAME")
	private String parentObjectName;
		
	@Column(name = "DESCRIPTION")
	private String description;
	
	@Column(name = "ATTRIBUTE1")
	private String attribute1;

	@Column(name = "ATTRIBUTE2")
	private String attribute2;

	@Column(name = "ATTRIBUTE3")
	private String attribute3;

	@Column(name = "ATTRIBUTE4")
	private String attribute4;

	@Column(name = "ATTRIBUTE5")
	private String attribute5;

	@Column(name = "CREATION_DATE")
	private Date creationDate;

	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;

	@Column(name = "CREATED_BY")
	private String createdBy;

	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy; 
}
