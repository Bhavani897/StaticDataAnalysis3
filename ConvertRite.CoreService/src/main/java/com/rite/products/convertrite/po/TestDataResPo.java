package com.rite.products.convertrite.po;

import java.util.List;
import java.util.Map;

public class TestDataResPo {

	private String message;
	private boolean isValidData;
	private String error;
	private List<Map<String, Object>> data;

	public List<Map<String, Object>> getData() {
		return data;
	}

	public void setData(List<Map<String, Object>> data) {
		this.data = data;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public boolean isValidData() {
		return isValidData;
	}

	public void setValidData(boolean isValidData) {
		this.isValidData = isValidData;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
