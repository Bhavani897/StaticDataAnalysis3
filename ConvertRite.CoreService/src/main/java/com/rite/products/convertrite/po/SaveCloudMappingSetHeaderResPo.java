package com.rite.products.convertrite.po;

public class SaveCloudMappingSetHeaderResPo {

	private long mapSetId;
	private String mapSetName;
	private String message;
	private String error;

	public long getMapSetId() {
		return mapSetId;
	}

	public void setMapSetId(long mapSetId) {
		this.mapSetId = mapSetId;
	}

	public String getMapSetName() {
		return mapSetName;
	}

	public void setMapSetName(String mapSetName) {
		this.mapSetName = mapSetName;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
