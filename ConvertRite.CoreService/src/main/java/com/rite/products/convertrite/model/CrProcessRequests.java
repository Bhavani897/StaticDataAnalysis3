package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.Data;

@Entity
@Table(name="CR_PROCESS_REQUESTS")
@Data
public class CrProcessRequests {
	
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "process_requests_generator")
	@SequenceGenerator(name = "process_requests_generator", sequenceName = "CR_TRANSFORM_REQ_ID_S", allocationSize = 1)
	@Column(name = "REQUEST_ID")
	private Long requestId;
	@Column(name="REQUEST_TYPE")
	private String requestType;
	@Column(name="CLD_TEMPLATE_ID")
	private Long cldTemplateId;
	@Column(name="SRC_TEMPLATE_ID")
	private Long srcTemplateId;
	@Column(name="CR_BATCH_NAME")
	private String crBatchName;
	@Column(name="STATUS")
	private String status;
	@Column(name="TOTAL_RECORDS")
	private Integer totalRecords;
	@Column(name = "COMPLETED_PERCENTAGE")
	private Integer percentage;
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
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy;
	
}
