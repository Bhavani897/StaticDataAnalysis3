package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrMappingValue;

public class SaveCloudMappingSetValuesResPo {

	private String message;
	private List<XxrMappingValue> cloudMappingSetColumns;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<XxrMappingValue> getCloudMappingSetColumns() {
		return cloudMappingSetColumns;
	}

	public void setCloudMappingSetColumns(List<XxrMappingValue> cloudMappingSetColumns) {
		this.cloudMappingSetColumns = cloudMappingSetColumns;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
