package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;

public class SourceTemplateColumnsId implements Serializable {

	private static final long serialVersionUID = 1L;

	@Column(name = "TEMPLATE_ID")
	private long templateId;
	@Column(name = "COLUMN_NAME")
	private String columnName;

	public long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(long templateId) {
		this.templateId = templateId;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

}
