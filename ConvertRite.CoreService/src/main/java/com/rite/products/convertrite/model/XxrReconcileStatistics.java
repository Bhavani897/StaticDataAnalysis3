package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_RECONCILE_STATISTICS")
public class XxrReconcileStatistics {

	@Id
	@Column(name="ID")
	private Long id;
	@Column(name="BATCH_NAME")
	private String batchName;
	@Column(name = "CLD_TEMPLATE_ID")
	private Long cldTemplateId;
	@Column(name="CLD_TEMPLATE_NAME")
	private String cldTemplateName;
	@Column(name = "SRC_TOTAL_COUNT")
	private Long extractionSucessRec;
	@Column(name = "EXT_SRC_PERCENTAGE")
	private Long extSrcPercentage;
	@Column(name = "SRC_VS_REC")
	private Long validationSucessRec;
	@Column(name = "SRC_VF_REC")
	private Long validationFailedRec;
	@Column(name = "VAL_PERCENTAGE")
	private Long validPercentage;
	@Column(name = "CLD_SUCC_REC")
	private Long convertedSuccRec;
	@Column(name = "CLD_FAILED_REC")
	private Long convertedFailRec;
	@Column(name = "CONV_PERCENTAGE")
	private Long convPercentage;
	
	

	public String getCldTemplateName() {
		return cldTemplateName;
	}

	public void setCldTemplateName(String cldTemplateName) {
		this.cldTemplateName = cldTemplateName;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getBatchName() {
		return batchName;
	}

	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}

	public Long getCldTemplateId() {
		return cldTemplateId;
	}

	public void setCldTemplateId(Long cldTemplateId) {
		this.cldTemplateId = cldTemplateId;
	}

	public Long getExtractionSucessRec() {
		return extractionSucessRec;
	}

	public void setExtractionSucessRec(Long extractionSucessRec) {
		this.extractionSucessRec = extractionSucessRec;
	}

	public Long getExtSrcPercentage() {
		return extSrcPercentage;
	}

	public void setExtSrcPercentage(Long extSrcPercentage) {
		this.extSrcPercentage = extSrcPercentage;
	}

	public Long getValidationSucessRec() {
		return validationSucessRec;
	}

	public void setValidationSucessRec(Long validationSucessRec) {
		this.validationSucessRec = validationSucessRec;
	}

	public Long getValidationFailedRec() {
		return validationFailedRec;
	}

	public void setValidationFailedRec(Long validationFailedRec) {
		this.validationFailedRec = validationFailedRec;
	}

	public Long getValidPercentage() {
		return validPercentage;
	}

	public void setValidPercentage(Long validPercentage) {
		this.validPercentage = validPercentage;
	}

	public Long getConvertedSuccRec() {
		return convertedSuccRec;
	}

	public void setConvertedSuccRec(Long convertedSuccRec) {
		this.convertedSuccRec = convertedSuccRec;
	}

	public Long getConvertedFailRec() {
		return convertedFailRec;
	}

	public void setConvertedFailRec(Long convertedFailRec) {
		this.convertedFailRec = convertedFailRec;
	}

	public Long getConvPercentage() {
		return convPercentage;
	}

	public void setConvPercentage(Long convPercentage) {
		this.convPercentage = convPercentage;
	}

}
