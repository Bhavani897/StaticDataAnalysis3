package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrLookUpValues;

public class SaveLookUpValuesResPo {

	private String message;
	private List<XxrLookUpValues> xxrLookUpValues;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<XxrLookUpValues> getXxrLookUpValues() {
		return xxrLookUpValues;
	}

	public void setXxrLookUpValues(List<XxrLookUpValues> xxrLookUpValues) {
		this.xxrLookUpValues = xxrLookUpValues;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
