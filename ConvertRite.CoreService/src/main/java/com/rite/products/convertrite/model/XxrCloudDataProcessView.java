package com.rite.products.convertrite.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name = "XXR_CLOUD_DATA_PROCESS_V")
public class XxrCloudDataProcessView {

	@Id
	@Column(name = "REQUEST_ID")
	private String requestId;
	@Lob
	@Column(name = "SQLQUERY")
	private String sqlQuery;
	@Column(name = "DESCRIPTION")
	private String description;
	@Column(name = "PATH")
	private String path;
	@Column(name = "STATUS")
	private String status;
	@Column(name = "LOOKUP_FLAG")
	private String lookupFlag;
	@Column(name = "SCHEDULED_JOB_CALL")
	private String scheduledJobCall;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "PRIORITY")
	private Integer priority;
	@Column(name = "POD_ID")
	private Long podId;
	@Column(name = "PROJECT_ID")
	private Long projectId;
	@Column(name = "POD_NAME")
	private String podName;
	@Column(name = "PROJECT_NAME")
	private String projectName;
	@Column(name = "META_DATA")
	private String metaData;

	@Column(name = "TABLE_NAME")
	private String tableName;

	@Column(name = "EXTRACTION_FLAG")
	private String extractionFlag;

	public String getMetaData() {
		return metaData;
	}

	public void setMetaData(String metaData) {
		this.metaData = metaData;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getExtractionFlag() {
		return extractionFlag;
	}

	public void setExtractionFlag(String extractionFlag) {
		this.extractionFlag = extractionFlag;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getLookupFlag() {
		return lookupFlag;
	}

	public void setLookupFlag(String lookupFlag) {
		this.lookupFlag = lookupFlag;
	}

	public String getScheduledJobCall() {
		return scheduledJobCall;
	}

	public void setScheduledJobCall(String scheduledJobCall) {
		this.scheduledJobCall = scheduledJobCall;
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

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
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

	public String getPodName() {
		return podName;
	}

	public void setPodName(String podName) {
		this.podName = podName;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

}
