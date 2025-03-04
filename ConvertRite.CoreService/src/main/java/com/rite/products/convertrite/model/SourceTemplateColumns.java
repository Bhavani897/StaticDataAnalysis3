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

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name = "XXR_SRC_TEMP_COLS")
//@IdClass(SourceTemplateColumnsId.class)
public class SourceTemplateColumns implements Serializable {

	private static final long serialVersionUID = 1L;
	// @Id
	@Column(name = "TEMPLATE_ID")
	private long templateId;
	// @Id
	@Column(name = "COLUMN_NAME")
	private String columnName;
	@Id
	@Column(name = "COLUMN_ID")
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "src_tmp_cols_gen")
	@SequenceGenerator(name = "src_tmp_cols_gen", sequenceName = "xxr_src_column_id_s", allocationSize = 1)
	private long columnId;
	@Column(name = "SELECTED")
	private String selected;
	@Column(name = "ORIG_TRANS_REF")
	private String origTransRef;
	/*
	 * @Column(name = "MAPPING_TYPE") private String mappingType;
	 * 
	 * @Column(name = "MAPPING_SET_ID") private Long mappingSetId;
	 * 
	 * @Column(name = "MAPPING_VALUE") private String mappingValue;
	 */
	@Column(name = "DISPLAY_SEQ")
	private Integer seq;
	@Column(name = "COLUMN_TYPE")
	private String columnType;
	@Column(name = "WIDTH")
	private Long width;
	/*
	 * @Column(name = "NULL_ALLOWED_FLAG") private String nullAllowedFlag;
	 * 
	 * @JsonFormat(shape = JsonFormat.Shape.STRING, pattern =
	 * "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	 * 
	 * @Column(name = "START_DATE") private Date startDate;
	 * 
	 * @JsonFormat(shape = JsonFormat.Shape.STRING, pattern =
	 * "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	 * 
	 * @Column(name = "END_DATE") private Date endDate;
	 * 
	 * @Column(name = "ENABLED_FLAG") private String enabledFlag;
	 */
	@Column(name = "ATTRIBUTE1")
	private String attribute1;
	@Column(name = "ATTRIBUTE2")
	private String attribute2;
	@Column(name = "ATTRIBUTE3")
	private String attribute3;
	@Column(name = "ATTRIBUTE4")
	private String attribute4;
	@Column(name = "ATTRIBUTE5")
	private String attribute5;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdatedBy;
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@Column(name = "CREATED_BY")
	private String createdBy;

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
