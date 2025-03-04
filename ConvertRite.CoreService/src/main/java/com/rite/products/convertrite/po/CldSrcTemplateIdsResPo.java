package com.rite.products.convertrite.po;

public class CldSrcTemplateIdsResPo {

	private Long cldTemplateId;
	private Long srcTemplateId;
	private String cldTemplateName;
	private String srcTemplateName;

	public String getSrcTemplateName() {
		return srcTemplateName;
	}

	public void setSrcTemplateName(String srcTemplateName) {
		this.srcTemplateName = srcTemplateName;
	}

	public String getCldTemplateName() {
		return cldTemplateName;
	}

	public void setCldTemplateName(String cldTemplateName) {
		this.cldTemplateName = cldTemplateName;
	}

	public CldSrcTemplateIdsResPo(Long cldTemplateId, Long srcTemplateId, String cldTemplateName,String srcTemplateName) {
		super();
		this.cldTemplateId = cldTemplateId;
		this.srcTemplateId = srcTemplateId;
		this.cldTemplateName = cldTemplateName;
		this.srcTemplateName=srcTemplateName;
	}

	public Long getCldTemplateId() {
		return cldTemplateId;
	}

	public void setCldTemplateId(Long cldTemplateId) {
		this.cldTemplateId = cldTemplateId;
	}

	public Long getSrcTemplateId() {
		return srcTemplateId;
	}

	public void setSrcTemplateId(Long srcTemplateId) {
		this.srcTemplateId = srcTemplateId;
	}

}
