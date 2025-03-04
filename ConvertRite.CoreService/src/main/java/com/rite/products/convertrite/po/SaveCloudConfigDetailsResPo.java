package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrCloudDataProcessConfig;

public class SaveCloudConfigDetailsResPo {

	private XxrCloudDataProcessConfig xxrCloudDataProcessConfig;
	private String message;
	private String error;

	public XxrCloudDataProcessConfig getXxrCloudDataProcessConfig() {
		return xxrCloudDataProcessConfig;
	}

	public void setXxrCloudDataProcessConfig(XxrCloudDataProcessConfig xxrCloudDataProcessConfig) {
		this.xxrCloudDataProcessConfig = xxrCloudDataProcessConfig;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
