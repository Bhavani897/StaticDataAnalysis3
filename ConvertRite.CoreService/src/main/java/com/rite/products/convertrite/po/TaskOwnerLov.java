package com.rite.products.convertrite.po;

public class TaskOwnerLov {

	private long taskOwnerId;
	private String taskOwnerName;

	public TaskOwnerLov(long taskOwnerId, String taskOwnerName) {
		super();
		this.taskOwnerId = taskOwnerId;
		this.taskOwnerName = taskOwnerName;
	}

	public long getTaskOwnerId() {
		return taskOwnerId;
	}

	public void setTaskOwnerId(long taskOwnerId) {
		this.taskOwnerId = taskOwnerId;
	}

	public String getTaskOwnerName() {
		return taskOwnerName;
	}

	public void setTaskOwnerName(String taskOwnerName) {
		this.taskOwnerName = taskOwnerName;
	}

}
