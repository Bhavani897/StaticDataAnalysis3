package com.rite.products.convertrite.po;

import java.util.List;

public class CommonLovPo {

	private long podId;
	private String podName;
	private List<ProjectsLovPo> projects;

	public long getPodId() {
		return podId;
	}

	public void setPodId(long podId) {
		this.podId = podId;
	}

	public String getPodName() {
		return podName;
	}

	public void setPodName(String podName) {
		this.podName = podName;
	}

	public List<ProjectsLovPo> getProjects() {
		return projects;
	}

	public void setProjects(List<ProjectsLovPo> projects) {
		this.projects = projects;
	}

	

}
