package com.rite.products.convertrite.po;

import java.sql.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class SaveCloudLookUpSetColumnsPo {

	private Long lookUpSetId;
	private String lookUpValue;
	private String enabledFlag;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	private Date endDate;
	private Long lookUpValueId;
	private String parentObjectValue;
	private String attribute1;
	private String attribute2;
	private String attribute3;

	public Long getLookUpSetId() {
		return lookUpSetId;
	}

	public void setLookUpSetId(Long lookUpSetId) {
		this.lookUpSetId = lookUpSetId;
	}

	public String getLookUpValue() {
		return lookUpValue;
	}

	public void setLookUpValue(String lookUpValue) {
		this.lookUpValue = lookUpValue;
	}

	public String getEnabledFlag() {
		return enabledFlag;
	}

	public void setEnabledFlag(String enabledFlag) {
		this.enabledFlag = enabledFlag;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public Long getLookUpValueId() {
		return lookUpValueId;
	}

	public void setLookUpValueId(Long lookUpValueId) {
		this.lookUpValueId = lookUpValueId;
	}

	public String getParentObjectValue() {
		return parentObjectValue;
	}

	public void setParentObjectValue(String parentObjectValue) {
		this.parentObjectValue = parentObjectValue;
	}

	public String getAttribute1() {
		return attribute1;
	}

	public void setAttribute1(String attribute1) {
		this.attribute1 = attribute1;
	}

	public String getAttribute2() {
		return attribute2;
	}

	public void setAttribute2(String attribute2) {
		this.attribute2 = attribute2;
	}

	public String getAttribute3() {
		return attribute3;
	}

	public void setAttribute3(String attribute3) {
		this.attribute3 = attribute3;
	}

}
