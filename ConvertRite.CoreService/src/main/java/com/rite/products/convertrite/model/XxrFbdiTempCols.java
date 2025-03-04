package com.rite.products.convertrite.model;

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
@Table(name = "XXR_FBDI_TEMP_COLS")
public class XxrFbdiTempCols {

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "fbdi_temp_cols_gen")
	@SequenceGenerator(name = "fbdi_temp_cols_gen", sequenceName = "xxr_fbdi_column_id_s", allocationSize = 1)
	@Column(name = "FBDI_COLUMN_ID")
	private long fbdiColumnId;
	@Column(name = "FBDI_TEMPLATE_ID")
	private long fbdiTemplateId;
	@Column(name = "FBDI_COLUMN_NAME")
	private String fbdiColumnName;
	@Column(name = "REQUIRED")
	private String required;
	@Column(name = "DATABASE_TABLE")
	private String databaseTable;
	@Column(name = "DATABASE_COLUMN")
	private String databaseColumn;
	@Column(name = "ACTIVE_FLAG")
	private String activeFlag;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "START_DATE")
	private Date startDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "END_DATE")
	private Date endDate;
	@Column(name = "SEQUENCE")
	private Integer sequence;
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdatedBy;
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@Column(name = "CREATED_BY")
	private String createdBy;

	public Date getLastUpdatedDate() {
		return lastUpdatedDate;
	}

	public void setLastUpdatedDate(Date lastUpdatedDate) {
		this.lastUpdatedDate = lastUpdatedDate;
	}

	public String getLastUpdatedBy() {
		return lastUpdatedBy;
	}

	public void setLastUpdatedBy(String lastUpdatedBy) {
		this.lastUpdatedBy = lastUpdatedBy;
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

	public Integer getSequence() {
		return sequence;
	}

	public void setSequence(Integer sequence) {
		this.sequence = sequence;
	}

	public long getFbdiColumnId() {
		return fbdiColumnId;
	}

	public void setFbdiColumnId(long fbdiColumnId) {
		this.fbdiColumnId = fbdiColumnId;
	}

	public long getFbdiTemplateId() {
		return fbdiTemplateId;
	}

	public void setFbdiTemplateId(long fbdiTemplateId) {
		this.fbdiTemplateId = fbdiTemplateId;
	}

	public String getFbdiColumnName() {
		return fbdiColumnName;
	}

	public void setFbdiColumnName(String fbdiColumnName) {
		this.fbdiColumnName = fbdiColumnName;
	}

	public String getRequired() {
		return required;
	}

	public void setRequired(String required) {
		this.required = required;
	}

	public String getDatabaseTable() {
		return databaseTable;
	}

	public void setDatabaseTable(String databaseTable) {
		this.databaseTable = databaseTable;
	}

	public String getDatabaseColumn() {
		return databaseColumn;
	}

	public void setDatabaseColumn(String databaseColumn) {
		this.databaseColumn = databaseColumn;
	}

	public String getActiveFlag() {
		return activeFlag;
	}

	public void setActiveFlag(String activeFlag) {
		this.activeFlag = activeFlag;
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

}
