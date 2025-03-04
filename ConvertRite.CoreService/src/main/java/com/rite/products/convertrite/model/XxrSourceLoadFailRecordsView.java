package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name = "XXR_SOURCE_LOAD_FAIL_RECORDS_V")
public class XxrSourceLoadFailRecordsView {

	@Id
	@Column(name = "ID")
	private Long id;
	@Column(name = "TEMPLATE_ID")
	private Long templateId;
	@Column(name = "FILE_NAME")
	private String fileName;
	@Column(name = "PARENT_OBJECT_CODE")
	private String parentObjectCode;
	@Column(name = "TEMPLATE_NAME")
	private String templateName;
	@Column(name = "SUCCESS")
	private long totalSuccessRec;
	@Column(name = "FAILED")
	private long totalFailedRec;
	@Column(name = "TOTAL_RECORDS")
	private long totalRecords;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "CREATION_DATE")
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date creationDate;
	@Column(name="DOWNLOAD_FLAG")
	private String downloadFlag;

	
	public String getDownloadFlag() {
		return downloadFlag;
	}

	public void setDownloadFlag(String downloadFlag) {
		this.downloadFlag = downloadFlag;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
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

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
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

}
