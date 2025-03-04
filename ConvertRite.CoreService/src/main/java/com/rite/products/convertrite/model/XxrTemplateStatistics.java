package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_TEMPLATE_STATISTICS")
public class XxrTemplateStatistics {
	@Id
	@Column(name = "CLOUD_TEMPLATE_NAME")
	private String cloudTemplateName;
	@Column(name = "POD_ID")
	private Long podId;
	@Column(name = "POD")
	private String podName;
	@Column(name = "PROJECT_ID")
	private Long projectId;
	@Column(name = "PROJECT")
	private String projectName;
	@Column(name = "PARENT_OBJECT_ID")
	private Long parentObjectId;
	@Column(name = "PARENT_OBJECT_CODE")
	private String parentObectCode;
	@Column(name = "OBJECT_ID")
	private Long objectId;
	@Column(name = "OBJECT")
	private String objectCode;
	@Column(name = "TOTAL_SOURCE_RECORDS")
	private Integer totalSourceRecords;
	@Column(name = "TOTAL_CLOUD_RECORDS")
	private Integer totalCloudRecords;
	@Column(name = "VALIDATION_FAILED")
	private Integer validationFailed;
	@Column(name = "VALIDATION_PASSED")
	private Integer validationPassed;
	@Column(name = "SOURCE_UNVERFIED")
	private Integer sourceUnverified;
	@Column(name = "VS_TRANS_REC")
	private Integer vsTransRec;
	@Column(name = "VS_UNTRANS_REC")
	private Integer vsUntransRec;
	@Column(name = "VF_TRANS_REC")
	private Integer vfTransRec;
	@Column(name = "VF_UNTRANS_REC")
	private Integer vfUntransRec;
	@Column(name = "CLOUD_UNKOWN_RECORDS")
	private Integer cloudUnknownRecords;
	@Column(name = "TOTAL_UNTRANS_REC")
	private Integer totalUnTransRec;

	public String getCloudTemplateName() {
		return cloudTemplateName;
	}

	public void setCloudTemplateName(String cloudTemplateName) {
		this.cloudTemplateName = cloudTemplateName;
	}

	public Long getPodId() {
		return podId;
	}

	public void setPodId(Long podId) {
		this.podId = podId;
	}

	public String getPodName() {
		return podName;
	}

	public void setPodName(String podName) {
		this.podName = podName;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public Long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(Long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public String getParentObectCode() {
		return parentObectCode;
	}

	public void setParentObectCode(String parentObectCode) {
		this.parentObectCode = parentObectCode;
	}

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
		this.objectId = objectId;
	}

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
	}

	public Integer getTotalSourceRecords() {
		return totalSourceRecords;
	}

	public void setTotalSourceRecords(Integer totalSourceRecords) {
		this.totalSourceRecords = totalSourceRecords;
	}

	public Integer getTotalCloudRecords() {
		return totalCloudRecords;
	}

	public void setTotalCloudRecords(Integer totalCloudRecords) {
		this.totalCloudRecords = totalCloudRecords;
	}

	public Integer getValidationFailed() {
		return validationFailed;
	}

	public void setValidationFailed(Integer validationFailed) {
		this.validationFailed = validationFailed;
	}

	public Integer getValidationPassed() {
		return validationPassed;
	}

	public void setValidationPassed(Integer validationPassed) {
		this.validationPassed = validationPassed;
	}

	public Integer getSourceUnverified() {
		return sourceUnverified;
	}

	public void setSourceUnverified(Integer sourceUnverified) {
		this.sourceUnverified = sourceUnverified;
	}

	public Integer getVsTransRec() {
		return vsTransRec;
	}

	public void setVsTransRec(Integer vsTransRec) {
		this.vsTransRec = vsTransRec;
	}

	public Integer getVsUntransRec() {
		return vsUntransRec;
	}

	public void setVsUntransRec(Integer vsUntransRec) {
		this.vsUntransRec = vsUntransRec;
	}

	public Integer getVfTransRec() {
		return vfTransRec;
	}

	public void setVfTransRec(Integer vfTransRec) {
		this.vfTransRec = vfTransRec;
	}

	public Integer getVfUntransRec() {
		return vfUntransRec;
	}

	public void setVfUntransRec(Integer vfUntransRec) {
		this.vfUntransRec = vfUntransRec;
	}

	public Integer getCloudUnknownRecords() {
		return cloudUnknownRecords;
	}

	public void setCloudUnknownRecords(Integer cloudUnknownRecords) {
		this.cloudUnknownRecords = cloudUnknownRecords;
	}

	public Integer getTotalUnTransRec() {
		return totalUnTransRec;
	}

	public void setTotalUnTransRec(Integer totalUnTransRec) {
		this.totalUnTransRec = totalUnTransRec;
	}

}
