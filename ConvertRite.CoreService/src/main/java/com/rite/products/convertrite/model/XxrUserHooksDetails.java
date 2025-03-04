package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name = "XXR_USER_HOOKS_DETAILS")
public class XxrUserHooksDetails {

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "xxr_user_hooks_generator")
	@SequenceGenerator(name = "xxr_user_hooks_generator", sequenceName = "XXR_USER_HOOKS_DETAILS_S", allocationSize = 1)
	@Column(name = "ID")
	private Long id;
	@Column(name = "CLD_TEMP_ID")
	private Long cldTemplateId;
	@Column(name = "CLD_TEMP_NAME")
	private String cldTemplateName;
	@Column(name = "SRC_TEMP_ID")
	private Long srcTemplateId;
	@Column(name = "SRC_TEMP_NAME")
	private String srcTemplateName;
	@Column(name = "OBJECT_ID")
	private Long objectId;
	@Column(name = "PARENT_OBJ_ID")
	private Long parentObjectId;
	@Column(name = "POD_ID")
	private Long podId;
	@Column(name = "PROJECT_ID")
	private Long projectId;
	@Column(name = "EXT_FLAG")
	private String extFlag;
	@Column(name = "VAL_FLAG")
	private String valFlag;
	@Column(name = "CLD_IMPORT_FLAG")
	private String cldImportFlag;
	@Lob
	@Column(name = "EXTRACTION_PRE_HOOK")
	private String extractionPreHook;
	@Lob
	@Column(name = "EXTRACTION_POST_HOOK")
	private String extractionPostHook;
	@Lob
	@Column(name = "VALIDATION_PRE_HOOK")
	private String validationPreHook;
	@Lob
	@Column(name = "VALIDATION_POST_HOOK")
	private String validationPostHook;
	@Lob
	@Column(name = "CLD_IMPORT_PRE_HOOK")
	private String cldImportPreHook;
	@Lob
	@Column(name = "CLD_IMPORT_POST_HOOK")
	private String cldImportPostHook;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "LAST_UPDATED_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy;

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

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public Date getLastUpdatedDate() {
		return lastUpdatedDate;
	}

	public void setLastUpdatedDate(Date lastUpdatedDate) {
		this.lastUpdatedDate = lastUpdatedDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getLastUpdateBy() {
		return lastUpdateBy;
	}

	public void setLastUpdateBy(String lastUpdateBy) {
		this.lastUpdateBy = lastUpdateBy;
	}

}
