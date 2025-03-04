package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data
@Table(name = "CR_CLD_TEMPLATE_HDRS_V")
public class CrCloudTemplateHeadersView {
	
	@Id
	@Column(name = "CLD_TEMPLATE_ID")
	private Long templateId;
	@Column(name = "CLD_TEMPLATE_NAME")
	private String templateName;
	@Column(name = "CLD_TEMPLATE_CODE")
	private String templateCode;
	@Column(name = "CLOUD_VERSION")
	private String version;
	@Column(name = "PROJECT_ID")
	private Long projectId;
	@Column(name = "PROJECT_NAME")
	private String projectName;
	@Column(name = "PARENT_OBJECT_ID")
	private Long parentObjectId;
	@Column(name = "PARENT_OBJECT_NAME")
	private String parentObjectCode;
	@Column(name = "OBJECT_ID")
	private Long objectId;
	@Column(name = "OBJECT_NAME")
	private String objectCode;
	@Column(name = "METADATA_TABLE_ID")
	private Long metaDataTableId;
	@Column(name = "METADATA_TABLE_NAME")
	private String metaDataTableName;
	@Column(name = "SRC_TEMPLATE_ID")
	private Long sourceTemplateId;
	@Column(name = "SOURCE_TEMPLATE_NAME")
	private String sourceTemplateName;
	@Column(name = "STAGING_TABLE_NAME")
	private String stagingTableName;
	@Column(name = "VIEW_NAME")
	private String viewName;
	@Column(name="PRIMARY_TEMPLATE_FLAG")
	private String primaryTemplateFlag;
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
}
