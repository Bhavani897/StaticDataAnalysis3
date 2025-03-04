package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrCloudConfig;

public class SaveCloudObjectRelatedConfigDetailsResPo {

	private XxrCloudConfig xxrCloudConfig;
	private String message;
	private String error;

	public XxrCloudConfig getXxrCloudConfig() {
		return xxrCloudConfig;
	}

	public void setXxrCloudConfig(XxrCloudConfig xxrCloudConfig) {
		this.xxrCloudConfig = xxrCloudConfig;
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
