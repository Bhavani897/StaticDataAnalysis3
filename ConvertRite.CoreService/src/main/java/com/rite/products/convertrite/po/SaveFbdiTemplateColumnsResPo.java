package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrFbdiTempCols;

public class SaveFbdiTemplateColumnsResPo {

	private String message;
	private List<XxrFbdiTempCols> fbdiTemplateColumns;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<XxrFbdiTempCols> getFbdiTemplateColumns() {
		return fbdiTemplateColumns;
	}

	public void setFbdiTemplateColumns(List<XxrFbdiTempCols> fbdiTemplateColumns) {
		this.fbdiTemplateColumns = fbdiTemplateColumns;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
