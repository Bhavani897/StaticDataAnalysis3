package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrCloudMasterLookupValues;

public class SaveCloudLookUpSetColumnsResPo {

	private String message;
	private List<XxrCloudMasterLookupValues> cloudMasterLookUpColumns;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<XxrCloudMasterLookupValues> getCloudMasterLookUpColumns() {
		return cloudMasterLookUpColumns;
	}

	public void setCloudMasterLookUpColumns(List<XxrCloudMasterLookupValues> cloudMasterLookUpColumns) {
		this.cloudMasterLookUpColumns = cloudMasterLookUpColumns;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
