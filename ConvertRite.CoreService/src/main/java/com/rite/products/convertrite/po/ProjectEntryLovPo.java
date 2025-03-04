package com.rite.products.convertrite.po;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class ProjectEntryLovPo {

	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> projectName;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> projectManager;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> programNumber;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> clientManager;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> kpiAggreagationLevel;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> projectStatus;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> accessLevel;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> iterationType;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> projectType;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> clientProjectNumber;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> taskType;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> relatedTo;
	
	

	public List<LovValuesPo> getRelatedTo() {
		return relatedTo;
	}

	public void setRelatedTo(List<LovValuesPo> relatedTo) {
		this.relatedTo = relatedTo;
	}

	public List<LovValuesPo> getTaskType() {
		return taskType;
	}

	public void setTaskType(List<LovValuesPo> taskType) {
		this.taskType = taskType;
	}

	public List<LovValuesPo> getIterationType() {
		return iterationType;
	}

	public void setIterationType(List<LovValuesPo> iterationType) {
		this.iterationType = iterationType;
	}

	public List<LovValuesPo> getProjectName() {
		return projectName;
	}

	public void setProjectName(List<LovValuesPo> projectName) {
		this.projectName = projectName;
	}

	public List<LovValuesPo> getProjectManager() {
		return projectManager;
	}

	public void setProjectManager(List<LovValuesPo> projectManager) {
		this.projectManager = projectManager;
	}

	public List<LovValuesPo> getProgramNumber() {
		return programNumber;
	}

	public void setProgramNumber(List<LovValuesPo> programNumber) {
		this.programNumber = programNumber;
	}

	public List<LovValuesPo> getClientManager() {
		return clientManager;
	}

	public void setClientManager(List<LovValuesPo> clientManager) {
		this.clientManager = clientManager;
	}

	public List<LovValuesPo> getKpiAggreagationLevel() {
		return kpiAggreagationLevel;
	}

	public void setKpiAggreagationLevel(List<LovValuesPo> kpiAggreagationLevel) {
		this.kpiAggreagationLevel = kpiAggreagationLevel;
	}

	public List<LovValuesPo> getProjectStatus() {
		return projectStatus;
	}

	public void setProjectStatus(List<LovValuesPo> projectStatus) {
		this.projectStatus = projectStatus;
	}

	public List<LovValuesPo> getAccessLevel() {
		return accessLevel;
	}

	public void setAccessLevel(List<LovValuesPo> accessLevel) {
		this.accessLevel = accessLevel;
	}

	public List<LovValuesPo> getProjectType() {
		return projectType;
	}

	public void setProjectType(List<LovValuesPo> projectType) {
		this.projectType = projectType;
	}

	public List<LovValuesPo> getClientProjectNumber() {
		return clientProjectNumber;
	}

	public void setClientProjectNumber(List<LovValuesPo> clientProjectNumber) {
		this.clientProjectNumber = clientProjectNumber;
	}

}
