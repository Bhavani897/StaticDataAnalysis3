package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_CLD_TEMPLATE_HDRS")
public class CrCloudTemplateHeaders {

	@Id
	@GeneratedValue(strategy= GenerationType.IDENTITY)
	@Column(name = "CLD_TEMPLATE_ID")
	private Long templateId;
	@Column(name = "CLD_TEMPLATE_NAME")
	private String templateName;
	@Column(name="CLD_TEMPLATE_CODE")
	private String templateCode;
	@Column(name = "PROJECT_ID")
	private Long projectId;
	@Column(name = "PARENT_OBJECT_ID")
	private Long parentObjectId;
	@Column(name = "OBJECT_ID")
	private Long objectId;
	@Column(name = "METADATA_TABLE_ID")
	private Long metaDataTableId;
	@Column(name = "SRC_TEMPLATE_ID")
	private Long sourceTemplateId;
	@Column(name = "CLOUD_VERSION")
	private String version;
	@Column(name = "STAGING_TABLE_NAME")
	private String stagingTableName;
	@Column(name="PRIMARY_TEMPLATE_FLAG")
	private String primaryTemplateFlag;
	@Column(name = "VIEW_NAME")
	private String viewName;
	@Column(name="ATTRIBUTE1")
	private String attribute1;
	@Column(name="ATTRIBUTE2")
	private String attribute2;
	@Column(name="ATTRIBUTE3")
	private String attribute3;
	@Column(name="ATTRIBUTE4")
	private String attribute4;
	@Column(name="ATTRIBUTE5")
	private String attribute5;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdatedBy;
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
}
