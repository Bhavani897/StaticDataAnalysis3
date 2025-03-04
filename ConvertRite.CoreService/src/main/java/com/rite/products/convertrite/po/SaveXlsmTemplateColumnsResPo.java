package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrXlsmTempCols;

public class SaveXlsmTemplateColumnsResPo {

	private String message;
	private List<XxrXlsmTempCols> xlsmTemplateColumns;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<XxrXlsmTempCols> getXlsmTemplateColumns() {
		return xlsmTemplateColumns;
	}

	public void setXlsmTemplateColumns(List<XxrXlsmTempCols> xlsmTemplateColumns) {
		this.xlsmTemplateColumns = xlsmTemplateColumns;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
