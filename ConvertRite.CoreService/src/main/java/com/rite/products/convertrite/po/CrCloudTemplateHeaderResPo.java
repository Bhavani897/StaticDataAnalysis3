package com.rite.products.convertrite.po;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class CrCloudTemplateHeaderResPo {
	public Long templateId;
	public String templateName;
	public String templateCode;
	public String version;
	public Long projectId;
	public Long parentObjectId;
	public Long objectId;
	public Long metaDataTableId;
	public Long sourceTemplateId;
	public String stagingTableName;
	private String viewName;
	private String primaryTemplateFlag;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;
}
