package com.rite.products.convertrite.po;

public class SaveTemplateGroupingReqPo {

	private Long id;
	private String templateIds;
	private Long groupId;
	 private String isZipped;
	private String version;

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

	public Long getGroupId() {
		return groupId;
	}

	public void setGroupId(Long groupId) {
		this.groupId = groupId;
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

}
