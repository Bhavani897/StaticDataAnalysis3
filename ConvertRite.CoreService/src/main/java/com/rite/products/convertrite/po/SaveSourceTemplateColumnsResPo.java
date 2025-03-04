package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.CrSourceTemplateColumns;
import com.rite.products.convertrite.model.SourceTemplateColumns;

public class SaveSourceTemplateColumnsResPo {

	private String message;
	private Object sourceTemplateColumns;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Object getSourceTemplateColumns() {
		return sourceTemplateColumns;
	}

	public void setSourceTemplateColumns(Object sourceTemplateColumns) {
		this.sourceTemplateColumns = sourceTemplateColumns;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
