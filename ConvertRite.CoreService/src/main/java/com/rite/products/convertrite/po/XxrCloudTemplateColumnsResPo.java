package com.rite.products.convertrite.po;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class XxrCloudTemplateColumnsResPo {

	private long templateId;
	private long columnId;
	private String columnName;
	private String selected;
	private Long sourceColumnId;
	private String sourceColumnName;
	private String mappingType;
	private Long mappingSetId;
	private String mappingSetName;
	//private String enabledFlag;
	private String mappingValue1;
	private String mappingValue2;
	private String mappingValue3;
	private String mappingValue4;
	private String mappingValue5;
	private Integer seq;
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
	private String origTransRef;
	private String description;

	
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

	public long getTemplateId() {
		return templateId;
	}

	public void setTemplateId(long templateId) {
		this.templateId = templateId;
	}

	public long getColumnId() {
		return columnId;
	}

	public void setColumnId(long columnId) {
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

	public String getSourceColumnName() {
		return sourceColumnName;
	}

	public void setSourceColumnName(String sourceColumnName) {
		this.sourceColumnName = sourceColumnName;
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

	public String getMappingSetName() {
		return mappingSetName;
	}

	public void setMappingSetName(String mappingSetName) {
		this.mappingSetName = mappingSetName;
	}

	/*
	 * public String getEnabledFlag() { return enabledFlag; }
	 * 
	 * public void setEnabledFlag(String enabledFlag) { this.enabledFlag =
	 * enabledFlag; }
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
	 */
}
