package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_LOOKUP_SETS")
public class CrLookUpSets {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "LOOKUP_SET_ID")
	private Long lookUpSetId;

	@Column(name = "LOOKUP_SET_NAME")
	private String lookUpSetName;

	@Column(name = "LOOKUP_SET_CODE")
	private String lookUpSetCode;
	
	@Column(name = "DESCRIPTION")
	private String description;

	@Column(name = "RELATED_TO")
	private String relatedTo;

	@Column(name = "LOOKUP_FLAG")
	private String lookUpFlag;

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
