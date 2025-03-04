package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_TEMPLATE_STATE")
public class XxrTemplateState {

	@Column(name = "TEMPLATE_ID")
	private Long templateId;
	@Id
	@Column(name = "TEMPLATE_NAME")
	private String templateName;
	@Column(name = "VALIDATION")
	private String validation;
	@Column(name = "REPROCESS")
	private String reprocess;
	@Column(name = "CONVERSION")
	private String conversion;
	@Column(name = "FILE_GEN")
	private String filegen;

	public Long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Long templateId) {
		this.templateId = templateId;
	}

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	public String getValidation() {
		return validation;
	}

	public void setValidation(String validation) {
		this.validation = validation;
	}

	public String getReprocess() {
		return reprocess;
	}

	public void setReprocess(String reprocess) {
		this.reprocess = reprocess;
	}

	public String getConversion() {
		return conversion;
	}

	public void setConversion(String conversion) {
		this.conversion = conversion;
	}

	public String getFilegen() {
		return filegen;
	}

	public void setFilegen(String filegen) {
		this.filegen = filegen;
	}

}
