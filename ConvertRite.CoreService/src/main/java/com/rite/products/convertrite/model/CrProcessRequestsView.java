package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Date;

@Entity
@Data
@Table(name = "CR_PROCESS_REQUESTS_V")
public class CrProcessRequestsView {

	@Id
	@Column(name = "REQUEST_ID")
	private Long requestId;
	@Column(name = "REQUEST_TYPE")
	private String requestType;
	@Column(name = "CLD_TEMPLATE_ID")
	private Long templateId;
	@Column(name = "CLD_TEMPLATE_NAME")
	private String templateName;
	@Column(name="CR_BATCH_NAME")
	private String batchName;
	@Column(name = "STATUS")
	private String status;
	@Column(name = "TOTAL_RECORDS")
	private Long totalRecords;
	@Column(name = "PERCENTAGE")
	private Long percentage;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "START_DATE")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "END_DATE")
	private Date endDate;
	@Column(name = "ERR_MSG")
	private String errorMsg;
	@Column(name = "USER_ID")
	private String userId;
	@Column(name = "PARENT_OBJECT_CODE")
	private String parentObjectCode;
	@Column(name = "SUCCESS_REC")
	private Long successRec;
	@Column(name = "FAIL_REC")
	private Long failRec;
	@Column(name = "CLOUD_RECORD_COUNT")
	private String cloudRecordCount;
	@Column(name = "LOAD_REQUEST_ID")
	private Long loadRequestId;
	@Column(name = "JOB_STATUS")
	private String jobStatus;
	@Column(name = "PARAMETER_LIST")
	private String parameterList;
	@Column(name = "OBJECT_ID")
	private Long objectId;
	@Column(name = "DOCUMENT_AUTHOR")
	private String documentAuthor;
	@Column(name = "DOCUMENT_TITLE")
	private String documentTitle;
	@Column(name = "DOCUMENT_SECURITY_GROUP")
	private String documentSecurityGroup;
	@Column(name = "DOCUMENT_ACCOUNT")
	private String documentAccount;
	@Column(name = "CONTENT_ID")
	private String contentId;
	@Column(name = "JOB_NAME")
	private String jobName;
	@Column(name = "INTERFACE_ID")
	private String interfaceId;
	@Column(name = "CLOUD_JOB_ERROR_MESSAGE")
	private String cloudJobReturnMessage;
	@Column(name = "ADDITIONAL_INFO")
	private String additionalInfo;

}
