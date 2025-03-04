package com.rite.products.convertrite.po;

import java.sql.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class XxrSourceLoadFailRecordsResPo {

	private Long id;
	private Long templateId;
	private String fileName;
	private String parentObjectCode;
	private String templateName;
	private long totalSuccessRec;
	private long totalFailedRec;
	private long totalRecords;
	private String createdBy;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date creationDate;
	
	
	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public Long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Long templateId) {
		this.templateId = templateId;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getTotalSuccessRec() {
		return totalSuccessRec;
	}

	public void setTotalSuccessRec(long totalSuccessRec) {
		this.totalSuccessRec = totalSuccessRec;
	}

	public long getTotalFailedRec() {
		return totalFailedRec;
	}

	public void setTotalFailedRec(long totalFailedRec) {
		this.totalFailedRec = totalFailedRec;
	}

	

	

}
