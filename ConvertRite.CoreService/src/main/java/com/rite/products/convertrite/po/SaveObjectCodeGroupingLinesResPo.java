package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrObjectCodeGroupingLines;

public class SaveObjectCodeGroupingLinesResPo {

	private List<XxrObjectCodeGroupingLines> xxrObjectGroupingLines;
	private String error;
	private String message;

	public List<XxrObjectCodeGroupingLines> getXxrObjectGroupingLines() {
		return xxrObjectGroupingLines;
	}

	public void setXxrObjectGroupingLines(List<XxrObjectCodeGroupingLines> xxrObjectGroupingLines) {
		this.xxrObjectGroupingLines = xxrObjectGroupingLines;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
