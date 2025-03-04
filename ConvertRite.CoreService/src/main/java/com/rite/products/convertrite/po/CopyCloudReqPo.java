package com.rite.products.convertrite.po;

public class CopyCloudReqPo {

	private String newTemplateName;
	private String cloudMetaDataTableName;
	private Long oldTemplateId;
	//private Long podId;
	private String oldVersion;
	private String newVersion;

	public String getCloudMetaDataTableName() {
		return cloudMetaDataTableName;
	}

	public void setCloudMetaDataTableName(String cloudMetaDataTableName) {
		this.cloudMetaDataTableName = cloudMetaDataTableName;
	}

	public String getNewTemplateName() {
		return newTemplateName;
	}

	public void setNewTemplateName(String newTemplateName) {
		this.newTemplateName = newTemplateName;
	}

	public Long getOldTemplateId() {
		return oldTemplateId;
	}

	public void setOldTemplateId(Long oldTemplateId) {
		this.oldTemplateId = oldTemplateId;
	}

//	public Long getPodId() {
//		return podId;
//	}
//
//	public void setPodId(Long podId) {
//		this.podId = podId;
//	}

	public String getOldVersion() {
		return oldVersion;
	}

	public void setOldVersion(String oldVersion) {
		this.oldVersion = oldVersion;
	}

	public String getNewVersion() {
		return newVersion;
	}

	public void setNewVersion(String newVersion) {
		this.newVersion = newVersion;
	}

}
