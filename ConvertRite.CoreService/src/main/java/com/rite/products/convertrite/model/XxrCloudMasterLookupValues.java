package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name="XXR_CLOUD_MASTER_LOOKUP_VALUES")
public class XxrCloudMasterLookupValues {
	
	
	@Column(name="LOOKUP_SET_ID")
	private Long lookUpSetId;
	@Column(name="LOOKUP_VALUE")
	private String lookUpValue;
	@Column(name="ENABLED")
	private String enabledFlag;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name="START_DATE")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name="END_DATE")
	private Date endDate;
	@Id
	@Column(name="LOOKUP_VALUE_ID")
	private Long lookUpValueId;
	@Column(name="PARENT_OBJECT_VALUE")
	private String parentObjectValue;
	@Column(name="ATTRIBUTE1")
	private String attribute1;
	@Column(name="ATTRIBUTE2")
	private String attribute2;
	@Column(name="ATTRIBUTE3")
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
