package com.rite.products.convertrite.po;

import java.util.Date;

public class TaskBreakDownPo {

	private Long id;
	private Date begin;
	private Date finish;
	private String name;
	private Integer progress;
	private Date plannedStart;
	private Date plannedFinish;
	private String project;
	private Long successor;
	//private String preReqTask;
	private Long predecessor;
	private String taskNum;
	
	public TaskBreakDownPo(Long id, Date begin, Date finish, String name, Integer progress, Date plannedStart,
			Date plannedFinish, String project, Long successor, Long predecessor,  String taskNum) {
		super();
		this.id = id;
		this.begin = begin;
		this.finish = finish;
		this.name = name;
		this.progress = progress;
		this.plannedStart = plannedStart;
		this.plannedFinish = plannedFinish;
		this.project = project;
		this.successor = successor;
		this.predecessor = predecessor;
		this.taskNum = taskNum;
		
	}
	
	

	public String getTaskNum() {
		return taskNum;
	}



	public void setTaskNum(String taskNum) {
		this.taskNum = taskNum;
	}



	public Long getPredecessor() {
		return predecessor;
	}

	public void setPredecessor(Long predecessor) {
		this.predecessor = predecessor;
	}

	/*
	 * public String getPreReqTask() { return preReqTask; }
	 * 
	 * public void setPreReqTask(String preReqTask) { this.preReqTask = preReqTask;
	 * }
	 */

	public Long getSuccessor() {
		return successor;
	}

	public void setSuccessor(Long successor) {
		this.successor = successor;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Date getBegin() {
		return begin;
	}

	public void setBegin(Date begin) {
		this.begin = begin;
	}

	public Date getFinish() {
		return finish;
	}

	public void setFinish(Date finish) {
		this.finish = finish;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getProgress() {
		return progress;
	}

	public void setProgress(Integer progress) {
		this.progress = progress;
	}

	public Date getPlannedStart() {
		return plannedStart;
	}

	public void setPlannedStart(Date plannedStart) {
		this.plannedStart = plannedStart;
	}

	public Date getPlannedFinish() {
		return plannedFinish;
	}

	public void setPlannedFinish(Date plannedFinish) {
		this.plannedFinish = plannedFinish;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}
}
