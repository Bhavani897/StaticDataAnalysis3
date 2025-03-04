package com.rite.products.convertrite.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Entity
@Data
@IdClass(CrcloudColumnId.class)
@Table(name = "CR_CLOUD_COLUMNS")
public class CrCloudColumns {

	@Id
	@Column(name = "COLUMN_ID")
	private Long columnId;

	@Column(name = "COLUMN_NAME")
	private String columnName;

	@Column(name = "PHYSICAL_COLUMN_NAME")
	private String physicalColumnName;

	@Column(name = "USER_COLUMN_NAME")
	private String userColumnName;

	@Column(name = "DESCRIPTION")
	private String description;
	@Id
	@Column(name = "TABLE_ID")
	private Long tableId;

	@Column(name = "OBJECT_ID")
	private Long objectId;

	@Column(name = "STATUS")
	private String status;

	@Column(name = "SHORT_NAME")
	private String shortName;

	@Column(name = "ORA_EDITION_CONTEXT")
	private String oraEditionContext;

	@Column(name = "COLUMN_SEQUENCE")
	private String columnSequence;

	@Column(name = "COLUMN_TYPE")
	private String columnType;

	@Column(name = "WIDTH")
	private String width;

	@Column(name = "NULL_ALLOWED_FLAG")
	private String nullAllowedFlag;

	@Column(name = "TRANSLATE_FLAG")
	private String translateFlag;

	@Column(name = "PRECISION")
	private String precision;

	@Column(name = "SCALE")
	private String scale;

	@Column(name = "DOMAIN_CODE")
	private String domainCode;

	@Column(name = "DENORM_PATH")
	private String denormPath;

	@Column(name = "ROUTING_MODE")
	private String routingMode;

	@Column(name = "CLOUD_VERSION")
	private String cloudVersion;

	@Column(name = "ELIGIBLE_TO_BE_SECURED")
	private String eligibleToBeSecured;

	@Column(name = "SECURITY_CLASSIFICATION")
	private String securityClassification;

	@Column(name = "SEC_CLASSIFICATION_OVERRIDE")
	private String secClassificationOverride;

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

	@Column(name = "CREATED_BY")
	private String createdBy;

	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdateDate;

	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdatedBy;
}
