package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

@Entity
@Table(name = "XXR_CLOUD_CONFIG")
public class XxrCloudConfig {
	@Id
	@Column(name = "OBJECT_CODE")
	private String objectCode;
	@Column(name = "XLSM_FILE_NAME")
	private String xlsmFileName;
	@Column(name = "SHEET_NAME")
	private String sheetName;
	@Column(name = "CTL_FILE_NAME")
	private String ctlFileName;
	@Column(name = "INTERFACE_TABLE_NAME")
	private String interfaceTableName;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy;
	@Column(name="REJECTION_TABLE_NAME")
	private String rejectionTableName;
	@Column(name="ESSJOB_STATUS_COL")
	private String essJobStatusCol;
	
	
	
	public String getEssJobStatusCol() {
		return essJobStatusCol;
	}

	public void setEssJobStatusCol(String essJobStatusCol) {
		this.essJobStatusCol = essJobStatusCol;
	}

	public String getRejectionTableName() {
		return rejectionTableName;
	}

	public void setRejectionTableName(String rejectionTableName) {
		this.rejectionTableName = rejectionTableName;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public Date getLastUpdatedDate() {
		return lastUpdatedDate;
	}

	public void setLastUpdatedDate(Date lastUpdatedDate) {
		this.lastUpdatedDate = lastUpdatedDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getLastUpdateBy() {
		return lastUpdateBy;
	}

	public void setLastUpdateBy(String lastUpdateBy) {
		this.lastUpdateBy = lastUpdateBy;
	}

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
	}

	public String getXlsmFileName() {
		return xlsmFileName;
	}

	public void setXlsmFileName(String xlsmFileName) {
		this.xlsmFileName = xlsmFileName;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public String getCtlFileName() {
		return ctlFileName;
	}

	public void setCtlFileName(String ctlFileName) {
		this.ctlFileName = ctlFileName;
	}

	public String getInterfaceTableName() {
		return interfaceTableName;
	}

	public void setInterfaceTableName(String interfaceTableName) {
		this.interfaceTableName = interfaceTableName;
	}

}
