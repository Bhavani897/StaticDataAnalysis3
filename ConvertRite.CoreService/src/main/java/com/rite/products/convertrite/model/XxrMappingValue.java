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
@Table(name = "XXR_MAPPING_VALUE")
public class XxrMappingValue {

	@Column(name = "MAP_SET_ID")
	private long mapSetId;
	@Column(name = "MAP_LINE_ID")
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "xxr_mapping_value_generator")
	@SequenceGenerator(name = "xxr_mapping_value_generator", sequenceName = "xxr_mapping_value_id_s", allocationSize = 1)
	private Long mapLineId;
	@Column(name = "SOURCE_FIELD1")
	private String sourceField1;
	@Column(name = "SOURCE_FIELD2")
	private String sourceField2;
	@Column(name = "SOURCE_FIELD3")
	private String sourceField3;
	@Column(name = "ENABLED_FLAG")
	private String enabled;
	@Column(name = "CLOUD_VALUE")
	private String cloudValue;
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
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "LAST_UPDATED_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy;

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

	public long getMapSetId() {
		return mapSetId;
	}

	public void setMapSetId(Long mapSetId) {
		this.mapSetId = mapSetId;
	}

	public Long getMapLineId() {
		return mapLineId;
	}

	public void setMapLineId(long mapLineId) {
		this.mapLineId = mapLineId;
	}

	public String getSourceField1() {
		return sourceField1;
	}

	public void setSourceField1(String sourceField1) {
		this.sourceField1 = sourceField1;
	}

	public String getSourceField2() {
		return sourceField2;
	}

	public void setSourceField2(String sourceField2) {
		this.sourceField2 = sourceField2;
	}

	public String getSourceField3() {
		return sourceField3;
	}

	public void setSourceField3(String sourceField3) {
		this.sourceField3 = sourceField3;
	}

	public String getEnabled() {
		return enabled;
	}

	public void setEnabled(String enabled) {
		this.enabled = enabled;
	}

	public String getCloudValue() {
		return cloudValue;
	}

	public void setCloudValue(String cloudValue) {
		this.cloudValue = cloudValue;
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
