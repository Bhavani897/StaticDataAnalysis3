package com.rite.products.convertrite.model;

import java.io.Serializable;
import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_CLD_TEMP_COLS")
//@IdClass(XxrCloudTemplateColumnsId.class)
public class XxrCloudTemplateColumns implements Serializable {

	private static final long serialVersionUID = 1L;
	// @Id
	@Column(name = "TEMPLATE_ID")
	private long templateId;
	@Id
	@Column(name = "COLUMN_ID")
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "cld_cols_gen")
	@SequenceGenerator(name = "cld_cols_gen", sequenceName = "xxr_cld_column_id_s", allocationSize = 1)
	private long columnId;
	// @Id
	@Column(name = "COLUMN_NAME")
	private String columnName;
	@Column(name = "SELECTED")
	private String selected;
	@Column(name = "SOURCE_COLUMN_ID")
	private Long sourceColumnId;
	@Column(name = "MAPPING_TYPE")
	private String mappingType;
	@Column(name = "MAPPING_SET_ID")
	private Long mappingSetId;
	/*
	 * @Column(name = "ENABLED_FLAG") private String enabledFlag;
	 */
	@Column(name = "MAPPING_VALUE1")
	private String mappingValue1;
	@Column(name = "MAPPING_VALUE2")
	private String mappingValue2;
	@Column(name = "MAPPING_VALUE3")
	private String mappingValue3;
	@Column(name = "MAPPING_VALUE4")
	private String mappingValue4;
	@Column(name = "MAPPING_VALUE5")
	private String mappingValue5;
	@Column(name = "DISPLAY_SEQ")
	private Integer seq;
	@Column(name = "COLUMN_TYPE")
	private String columnType;
	@Column(name = "WIDTH")
	private Long width;
	@Column(name = "NULL_ALLOWED_FLAG")
	private String nullAllowedFlag;
	/*
	 * @JsonFormat(shape = JsonFormat.Shape.STRING, pattern =
	 * "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	 * 
	 * @Column(name = "START_DATE") private Date startDate;
	 * 
	 * @JsonFormat(shape = JsonFormat.Shape.STRING, pattern =
	 * "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	 * 
	 * @Column(name = "END_DATE") private Date endDate;
	 */
	@Column(name = "ORIG_TRANS_REF")
	private String origTransRef;
	@Column(name = "DESCRIPTION")
	private String description;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdatedBy;
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@Column(name = "CREATED_BY")
	private String createdBy;

	
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

	public String getLastUpdatedBy() {
		return lastUpdatedBy;
	}

	public void setLastUpdatedBy(String lastUpdatedBy) {
		this.lastUpdatedBy = lastUpdatedBy;
	}

	public Date getLastUpdatedDate() {
		return lastUpdatedDate;
	}

	public void setLastUpdatedDate(Date lastUpdatedDate) {
		this.lastUpdatedDate = lastUpdatedDate;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
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
