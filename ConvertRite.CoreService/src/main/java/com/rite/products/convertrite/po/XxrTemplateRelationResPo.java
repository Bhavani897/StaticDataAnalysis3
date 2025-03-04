package com.rite.products.convertrite.po;

public class XxrTemplateRelationResPo {

	private Long id;
	private String templateIds;
	private String isZipped;
	private String version;
	private Long groupId;
	private String groupName;
	private String templateNames;
	
	
	public String getTemplateNames() {
		return templateNames;
	}

	public void setTemplateNames(String templateNames) {
		this.templateNames = templateNames;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getTemplateIds() {
		return templateIds;
	}

	public void setTemplateIds(String templateIds) {
		this.templateIds = templateIds;
	}

	public String getIsZipped() {
		return isZipped;
	}

	public void setIsZipped(String isZipped) {
		this.isZipped = isZipped;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public Long getGroupId() {
		return groupId;
	}

	public void setGroupId(Long groupId) {
		this.groupId = groupId;
	}

}
