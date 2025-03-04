package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrCloudTemplateColumns;

public class SaveCloudTemplateColumnsResPo {

	private String message;
	private List<XxrCloudTemplateColumns> cloudTemplateColumns;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<XxrCloudTemplateColumns> getCloudTemplateColumns() {
		return cloudTemplateColumns;
	}

	public void setCloudTemplateColumns(List<XxrCloudTemplateColumns> cloudTemplateColumns) {
		this.cloudTemplateColumns = cloudTemplateColumns;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
