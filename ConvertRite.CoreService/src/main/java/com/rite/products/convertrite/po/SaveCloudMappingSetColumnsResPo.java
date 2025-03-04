package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrMappingValues;

public class SaveCloudMappingSetColumnsResPo {

	private String message;
	private List<XxrMappingValues> cloudMappingSetColumns;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<XxrMappingValues> getCloudMappingSetColumns() {
		return cloudMappingSetColumns;
	}

	public void setCloudMappingSetColumns(List<XxrMappingValues> cloudMappingSetColumns) {
		this.cloudMappingSetColumns = cloudMappingSetColumns;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
