package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "xxr_process_jobs")
public class XxrProcessJobs {

	@Id
	@Column(name = "REQUEST_ID")
	private Long requestId;
	@Column(name = "JOB_ID")
	private int jobId;
	@Column(name = "JOB_NAME")
	private String jobName;
	@Column(name = "JOB_STATUS")
	private String jobStatus;
	@Column(name = "WEIGHTAGE")
	private Integer weightage;
	@Column(name = "SUCCESS_RECORDS")
	private Integer successRecords;
	@Column(name = "FAILURE_RECORDS")
	private Integer failureRecords;

	public Long getRequestId() {
		return requestId;
	}

	public void setRequestId(Long requestId) {
		this.requestId = requestId;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
	}

	public Integer getWeightage() {
		return weightage;
	}

	public void setWeightage(Integer weightage) {
		this.weightage = weightage;
	}

	public Integer getSuccessRecords() {
		return successRecords;
	}

	public void setSuccessRecords(Integer successRecords) {
		this.successRecords = successRecords;
	}

	public Integer getFailureRecords() {
		return failureRecords;
	}

	public void setFailureRecords(Integer failureRecords) {
		this.failureRecords = failureRecords;
	}

}
