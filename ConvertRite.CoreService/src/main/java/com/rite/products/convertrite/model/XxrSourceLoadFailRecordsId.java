package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;

public class XxrSourceLoadFailRecordsId implements Serializable {

	private static final long serialVersionUID = 1L;

	@Column(name = "TEMPLATE_ID")
	private long templateId;
	
	@Column(name="ID")
	private Long id;

	public long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(long templateId) {
		this.templateId = templateId;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	

}
