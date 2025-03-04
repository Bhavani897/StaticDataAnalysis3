package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_CLOUD_TABLES")
public class CrCloudTables {
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "CR_CLD_TABLE_ID_SEQ")
	@SequenceGenerator(name = "CR_CLD_TABLE_ID_SEQ", sequenceName = "CR_CLD_TABLE_ID_S", allocationSize = 1)
	@Column(name = "TABLE_ID", nullable = false)
	private Long tableId;

	@Column(name = "TABLE_NAME", nullable = false)
	private String tableName;

	@Column(name = "PHYSICAL_TABLE_NAME", nullable = false)
	private String physicalTableName;

	@Column(name = "USER_TABLE_NAME", nullable = false)
	private String userTableName;

	@Column(name = "DESCRIPTION")
	private String description;

	@Column(name = "APPLICATION_SHORT_NAME", nullable = false)
	private String applicationShortName;

	@Column(name = "TABLE_TYPE")
	private String tableType;

	@Column(name = "HOSTED_SUPPORT_STYLE")
	private String hostedSupportStyle;

	@Column(name = "LOGICAL")
	private String logical;

	@Column(name = "MLS_SUPPORT_MODEL")
	private String mlsSupportModel;

	@Column(name = "STATUS")
	private String status;

	@Column(name = "DEPLOY_TO")
	private String deployTo;

	@Column(name = "EXTENSION_OF_TABLE")
	private String extensionOfTable;

	@Column(name = "SHORT_NAME")
	private String shortName;

	@Column(name = "SHARED_OBJECT")
	private String sharedObject;

	@Column(name = "CONFLICT_RESOLUTION")
	private String conflictResolution;

	@Column(name = "TABLESPACE_TYPE")
	private String tablespaceType;

	@Column(name = "SELECT_ALLOWED")
	private String selectAllowed;

	@Column(name = "INSERT_ALLOWED")
	private String insertAllowed;

	@Column(name = "UPDATE_ALLOWED")
	private String updateAllowed;

	@Column(name = "DELETE_ALLOWED")
	private String deleteAllowed;

	@Column(name = "TRUNCATE_ALLOWED")
	private String truncateAllowed;

	@Column(name = "MAINTAIN_PARTITION")
	private String maintainPartition;

	@Column(name = "EXCHANGE_PARTITION")
	private String exchangePartition;

	@Column(name = "MAINTAIN_INDEX")
	private String maintainIndex;

	@Column(name = "FLASHBACK_ALLOWED")
	private String flashbackAllowed;

	@Column(name = "ENABLE_AUDIT")
	private String enableAudit;

	@Column(name = "ORA_EDITION_CONTEXT")
	private String oraEditionContext;

	@Column(name = "OBJECT_ID")
	private Long objectId;

	@Column(name = "PARENT_OBJECT_ID")
	private String parentObjectId;

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

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "LAST_UPDATE_DATE", nullable = false)
	private Date lastUpdateDate;

	@Column(name = "LAST_UPDATED_BY", nullable = false)
	private String lastUpdatedBy;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name = "CREATION_DATE", nullable = false)
	private Date creationDate;

	@Column(name = "CREATED_BY", nullable = false)
	private String createdBy;

}
