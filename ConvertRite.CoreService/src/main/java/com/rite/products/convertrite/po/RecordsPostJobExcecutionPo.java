package com.rite.products.convertrite.po;

public class RecordsPostJobExcecutionPo {

	private Long cloudTemplateId;
	private String cloudTableName;
	private Long sourceTemplateId;
	private String sourceTableName;

	public RecordsPostJobExcecutionPo(Long cloudTemplateId, String cloudTableName, Long sourceTemplateId,
			String sourceTableName) {
		super();
		this.cloudTemplateId = cloudTemplateId;
		this.cloudTableName = cloudTableName;
		this.sourceTemplateId = sourceTemplateId;
		this.sourceTableName = sourceTableName;
	}

	public Long getCloudTemplateId() {
		return cloudTemplateId;
	}

	public void setCloudTemplateId(Long cloudTemplateId) {
		this.cloudTemplateId = cloudTemplateId;
	}

	public String getCloudTableName() {
		return cloudTableName;
	}

	public void setCloudTableName(String cloudTableName) {
		this.cloudTableName = cloudTableName;
	}

	public Long getSourceTemplateId() {
		return sourceTemplateId;
	}

	public void setSourceTemplateId(Long sourceTemplateId) {
		this.sourceTemplateId = sourceTemplateId;
	}

	public String getSourceTableName() {
		return sourceTableName;
	}

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

}
