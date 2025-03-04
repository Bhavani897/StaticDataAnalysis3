package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name = "XXR_PROCESS_REQUESTS_V")
public class XxrProcessRequestsView {

	@Id
	@Column(name = "REQUEST_ID")
	private Long requestId;
	@Column(name = "TEMPLATE_ID")
	private Long templateId;
	@Column(name = "TEMPLATE_NAME")
	private String templateName;
	@Column(name = "PARENT_OBJECT_ID")
	private Long parentObjectId;
	@Column(name = "PARENT_OBJECT_CODE")
	private String parentObjectCode;
	@Column(name = "REQUEST_TYPE")
	private String requestType;
	@Column(name = "STATUS")
	private String status;
	@Column(name = "TOTAL_RECORDS")
	private Long totalRecords;
	@Column(name = "PERCENTAGE")
	private Long percentage;
	@Column(name="XXR_BATCH_NAME")
	private String xxrBatchName;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "START_DATE")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "END_DATE")
	private Date endDate;
	@Column(name = "ERR_MSG")
	private String errorMsg;
	@Column(name = "POD_ID")
	private Integer podId;
	@Column(name = "USER_ID")
	private Integer userId;
	@Column(name = "SUCCESS_REC")
	private Long successRec;
	@Column(name = "FAIL_REC")
	private Long failRec;

	
	public String getXxrBatchName() {
		return xxrBatchName;
	}

	public void setXxrBatchName(String xxrBatchName) {
		this.xxrBatchName = xxrBatchName;
	}

	public Long getRequestId() {
		return requestId;
	}

	public void setRequestId(Long requestId) {
		this.requestId = requestId;
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

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

	public String getRequestType() {
		return requestType;
	}

	public void setRequestType(String requestType) {
		this.requestType = requestType;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(Long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public Long getPercentage() {
		return percentage;
	}

	public void setPercentage(Long percentage) {
		this.percentage = percentage;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}

	public Integer getPodId() {
		return podId;
	}

	public void setPodId(Integer podId) {
		this.podId = podId;
	}

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public Long getSuccessRec() {
		return successRec;
	}

	public void setSuccessRec(Long successRec) {
		this.successRec = successRec;
	}

	public Long getFailRec() {
		return failRec;
	}

	public void setFailRec(Long failRec) {
		this.failRec = failRec;
	}

}
