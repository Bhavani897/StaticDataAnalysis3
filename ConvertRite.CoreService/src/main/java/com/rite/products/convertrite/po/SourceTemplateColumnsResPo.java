package com.rite.products.convertrite.po;

import java.sql.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class SourceTemplateColumnsResPo {

	private long templateId;
	private String columnName;
	private long columnId;
	private String selected;
	private String origTransRef;
	/*
	 * private String mappingType; private Long mappingSetId; private String
	 * mappingSetName; private String mappingValue;
	 */
	private Integer seq;
	private String columnType;
	private Long width;
	/*
	 * private String nullAllowedFlag;
	 * 
	 * @JsonFormat(shape = JsonFormat.Shape.STRING, pattern =
	 * "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata") private Date
	 * startDate;
	 * 
	 * @JsonFormat(shape = JsonFormat.Shape.STRING, pattern =
	 * "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata") private Date
	 * endDate; private String enabledFlag;
	 */
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;

	
	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public Long getWidth() {
		return width;
	}

	public void setWidth(Long width) {
		this.width = width;
	}

	/*
	 * public String getNullAllowedFlag() { return nullAllowedFlag; }
	 * 
	 * public void setNullAllowedFlag(String nullAllowedFlag) { this.nullAllowedFlag
	 * = nullAllowedFlag; }
	 */

	public long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(long templateId) {
		this.templateId = templateId;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public long getColumnId() {
		return columnId;
	}

	public void setColumnId(long columnId) {
		this.columnId = columnId;
	}

	public String getSelected() {
		return selected;
	}

	public void setSelected(String selected) {
		this.selected = selected;
	}

	public String getOrigTransRef() {
		return origTransRef;
	}

	public void setOrigTransRef(String origTransRef) {
		this.origTransRef = origTransRef;
	}

	/*
	 * public String getMappingType() { return mappingType; }
	 * 
	 * public void setMappingType(String mappingType) { this.mappingType =
	 * mappingType; }
	 * 
	 * public Long getMappingSetId() { return mappingSetId; }
	 * 
	 * public void setMappingSetId(Long mappingSetId) { this.mappingSetId =
	 * mappingSetId; }
	 * 
	 * public String getMappingSetName() { return mappingSetName; }
	 * 
	 * public void setMappingSetName(String mappingSetName) { this.mappingSetName =
	 * mappingSetName; }
	 * 
	 * public String getMappingValue() { return mappingValue; }
	 * 
	 * public void setMappingValue(String mappingValue) { this.mappingValue =
	 * mappingValue; }
	 */

	public Integer getSeq() {
		return seq;
	}

	public void setSeq(Integer seq) {
		this.seq = seq;
	}

	/*
	 * public Date getStartDate() { return startDate; }
	 * 
	 * public void setStartDate(Date startDate) { this.startDate = startDate; }
	 * 
	 * public Date getEndDate() { return endDate; }
	 * 
	 * public void setEndDate(Date endDate) { this.endDate = endDate; }
	 * 
	 * public String getEnabledFlag() { return enabledFlag; }
	 * 
	 * public void setEnabledFlag(String enabledFlag) { this.enabledFlag =
	 * enabledFlag; }
	 */
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

	public String getAttribute4() {
		return attribute4;
	}

	public void setAttribute4(String attribute4) {
		this.attribute4 = attribute4;
	}

	public String getAttribute5() {
		return attribute5;
	}

	public void setAttribute5(String attribute5) {
		this.attribute5 = attribute5;
	}

}
