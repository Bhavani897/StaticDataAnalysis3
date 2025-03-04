package com.rite.products.convertrite.po;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class XxrProjectsResPo {

	private Long projectId;
	private String projectName;
	private String description;
	private Long podId;
	private String podName;
	private String projecManager;
	private String clientManager;
	private String kpiAggLevel;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date endDate;
	private String projectStatus;
	private String accessLevel;
	private String clientProjectNumber;
	private String programNumber;
	
	public XxrProjectsResPo(Long projectId, String projectName, String description, Long podId, String podName,
			String projecManager, String clientManager, String kpiAggLevel, Date startDate, Date endDate,
			String projectStatus, String accessLevel, String clientProjectNumber,
			String programNumber) {
		super();
		this.projectId = projectId;
		this.projectName = projectName;
		this.description = description;
		this.podId = podId;
		this.podName = podName;
		this.projecManager = projecManager;
		this.clientManager = clientManager;
		this.kpiAggLevel = kpiAggLevel;
		this.startDate = startDate;
		this.endDate = endDate;
		this.projectStatus = projectStatus;
		this.accessLevel = accessLevel;
		this.clientProjectNumber = clientProjectNumber;
		this.programNumber = programNumber;
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

	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getPodName() {
		return podName;
	}

	public void setPodName(String podName) {
		this.podName = podName;
	}

	public String getProjecManager() {
		return projecManager;
	}

	public void setProjecManager(String projecManager) {
		this.projecManager = projecManager;
	}

	public String getClientManager() {
		return clientManager;
	}

	public void setClientManager(String clientManager) {
		this.clientManager = clientManager;
	}

	public String getKpiAggLevel() {
		return kpiAggLevel;
	}

	public void setKpiAggLevel(String kpiAggLevel) {
		this.kpiAggLevel = kpiAggLevel;
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

	public String getProjectStatus() {
		return projectStatus;
	}

	public void setProjectStatus(String projectStatus) {
		this.projectStatus = projectStatus;
	}

	public String getAccessLevel() {
		return accessLevel;
	}

	public void setAccessLevel(String accessLevel) {
		this.accessLevel = accessLevel;
	}

	public String getClientProjectNumber() {
		return clientProjectNumber;
	}

	public void setClientProjectNumber(String clientProjectNumber) {
		this.clientProjectNumber = clientProjectNumber;
	}

	public String getProgramNumber() {
		return programNumber;
	}

	public void setProgramNumber(String programNumber) {
		this.programNumber = programNumber;
	}

}
