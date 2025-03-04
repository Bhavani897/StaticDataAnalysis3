package com.rite.products.convertrite.po;

public class PodsPo {

	private long podId;
	private String podName;

	public PodsPo() {
		super();
	}

	public PodsPo(long podId, String podName) {
		super();
		this.podId = podId;
		this.podName = podName;
	}

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

}
