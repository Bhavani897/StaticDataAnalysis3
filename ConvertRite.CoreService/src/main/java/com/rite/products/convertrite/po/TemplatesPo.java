package com.rite.products.convertrite.po;

public class TemplatesPo {

	private Long templateId;
	private String templateName;

	public TemplatesPo(Long templateId, String templateName) {
		super();
		this.templateId = templateId;
		this.templateName = templateName;
	}

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

	
	
}
