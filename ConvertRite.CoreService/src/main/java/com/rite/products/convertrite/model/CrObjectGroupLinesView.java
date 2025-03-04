package com.rite.products.convertrite.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Table;
import javax.persistence.Id;

import lombok.Data;

@Data
@Entity
@Table(name = "CR_OBJECT_GROUP_LINES_V")
public class CrObjectGroupLinesView {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "OBJ_GRP_LINE_ID")
	private Long objGrpLineId;
	
	@Column(name = "GROUP_ID")
	private Long groupId;
	
	@Column(name = "OBJECT_ID")
	private Long objectId;
	
	@Column(name = "OBJECT_NAME")
	private String objectName;
	
	@Column(name = "SEQUENCE")
	private Long sequence;
	
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
