package com.rite.products.convertrite.po;

import java.sql.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class SaveCloudTemplateColumnsPo {

	private Long templateId;
	private Long columnId;
	private String columnName;
	private String selected;
	private Long sourceColumnId;
	private String mappingType;
	private Long mappingSetId;
	private String mappingValue1;
	private String mappingValue2;
	private String mappingValue3;
	private String mappingValue4;
	private String mappingValue5;
	private Integer displaySeq;
	/*
	 * @JsonFormat(shape = JsonFormat.Shape.STRING, pattern =
	 * "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata") private Date
	 * startDate;
	 * 
	 * @JsonFormat(shape = JsonFormat.Shape.STRING, pattern =
	 * "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata") private Date
	 * endDate;
	 */
	private String columnType;
	private Long width;
	private String nullAllowedFlag;
	// private String enableFlag;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;
	private String origTransRef;
	private String description;

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getOrigTransRef() {
		return origTransRef;
	}

	public void setOrigTransRef(String origTransRef) {
		this.origTransRef = origTransRef;
	}

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

	public String getNullAllowedFlag() {
		return nullAllowedFlag;
	}

	public void setNullAllowedFlag(String nullAllowedFlag) {
		this.nullAllowedFlag = nullAllowedFlag;
	}

	public Long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Long templateId) {
		this.templateId = templateId;
	}

	public Long getColumnId() {
		return columnId;
	}

	public void setColumnId(Long columnId) {
		this.columnId = columnId;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getSelected() {
		return selected;
	}

	public void setSelected(String selected) {
		this.selected = selected;
	}

	public Long getSourceColumnId() {
		return sourceColumnId;
	}

	public void setSourceColumnId(Long sourceColumnId) {
		this.sourceColumnId = sourceColumnId;
	}

	public String getMappingType() {
		return mappingType;
	}

	public void setMappingType(String mappingType) {
		this.mappingType = mappingType;
	}

	public Long getMappingSetId() {
		return mappingSetId;
	}

	public void setMappingSetId(Long mappingSetId) {
		this.mappingSetId = mappingSetId;
	}

	public String getMappingValue1() {
		return mappingValue1;
	}

	public void setMappingValue1(String mappingValue1) {
		this.mappingValue1 = mappingValue1;
	}

	public String getMappingValue2() {
		return mappingValue2;
	}

	public void setMappingValue2(String mappingValue2) {
		this.mappingValue2 = mappingValue2;
	}

	public String getMappingValue3() {
		return mappingValue3;
	}

	public void setMappingValue3(String mappingValue3) {
		this.mappingValue3 = mappingValue3;
	}

	public String getMappingValue4() {
		return mappingValue4;
	}

	public void setMappingValue4(String mappingValue4) {
		this.mappingValue4 = mappingValue4;
	}

	public String getMappingValue5() {
		return mappingValue5;
	}

	public void setMappingValue5(String mappingValue5) {
		this.mappingValue5 = mappingValue5;
	}

	public Integer getDisplaySeq() {
		return displaySeq;
	}

	public void setDisplaySeq(Integer displaySeq) {
		this.displaySeq = displaySeq;
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
	 * public String getEnableFlag() { return enableFlag; }
	 * 
	 * public void setEnableFlag(String enableFlag) { this.enableFlag = enableFlag;
	 * }
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
