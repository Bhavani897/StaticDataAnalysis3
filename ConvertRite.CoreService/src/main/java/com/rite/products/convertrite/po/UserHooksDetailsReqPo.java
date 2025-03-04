package com.rite.products.convertrite.po;

public class UserHooksDetailsReqPo {

	private Long id;
	private Long cldTemplateId;
	private String cldTemplateName;
	private Long srcTemplateId;
	private String srcTemplateName;
	private Long objectId;
	private Long parentObjectId;
	private Long podId;
	private Long projectId;
	private String extFlag;
	private String valFlag;
	private String cldImportFlag;
	private String extractionPreHook;
	private String extractionPostHook;
	private String validationPreHook;
	private String validationPostHook;
	private String cldImportPreHook;
	private String cldImportPostHook;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getCldTemplateId() {
		return cldTemplateId;
	}

	public void setCldTemplateId(Long cldTemplateId) {
		this.cldTemplateId = cldTemplateId;
	}

	public String getCldTemplateName() {
		return cldTemplateName;
	}

	public void setCldTemplateName(String cldTemplateName) {
		this.cldTemplateName = cldTemplateName;
	}

	public Long getSrcTemplateId() {
		return srcTemplateId;
	}

	public void setSrcTemplateId(Long srcTemplateId) {
		this.srcTemplateId = srcTemplateId;
	}

	public String getSrcTemplateName() {
		return srcTemplateName;
	}

	public void setSrcTemplateName(String srcTemplateName) {
		this.srcTemplateName = srcTemplateName;
	}

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
		this.objectId = objectId;
	}

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public String getExtFlag() {
		return extFlag;
	}

	public void setExtFlag(String extFlag) {
		this.extFlag = extFlag;
	}

	public String getValFlag() {
		return valFlag;
	}

	public void setValFlag(String valFlag) {
		this.valFlag = valFlag;
	}

	public String getCldImportFlag() {
		return cldImportFlag;
	}

	public void setCldImportFlag(String cldImportFlag) {
		this.cldImportFlag = cldImportFlag;
	}

	public String getExtractionPreHook() {
		return extractionPreHook;
	}

	public void setExtractionPreHook(String extractionPreHook) {
		this.extractionPreHook = extractionPreHook;
	}

	public String getExtractionPostHook() {
		return extractionPostHook;
	}

	public void setExtractionPostHook(String extractionPostHook) {
		this.extractionPostHook = extractionPostHook;
	}

	public String getValidationPreHook() {
		return validationPreHook;
	}

	public void setValidationPreHook(String validationPreHook) {
		this.validationPreHook = validationPreHook;
	}

	public String getValidationPostHook() {
		return validationPostHook;
	}

	public void setValidationPostHook(String validationPostHook) {
		this.validationPostHook = validationPostHook;
	}

	public String getCldImportPreHook() {
		return cldImportPreHook;
	}

	public void setCldImportPreHook(String cldImportPreHook) {
		this.cldImportPreHook = cldImportPreHook;
	}

	public String getCldImportPostHook() {
		return cldImportPostHook;
	}

	public void setCldImportPostHook(String cldImportPostHook) {
		this.cldImportPostHook = cldImportPostHook;
	}

}
