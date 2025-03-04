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
@Table(name = "XXR_SRC_LOOKUP_SETS")
public class XxrSrcLookupSets {

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "src_lookup_sets_generator")
	@SequenceGenerator(name = "src_lookup_sets_generator", sequenceName = "xxr_src_lookup_sets_id_s", allocationSize = 1)
	@Column(name = "LOOKUP_SET_ID")
	private long lookupsetId;
	@Column(name = "LOOKUP_SET_NAME")
	private String lookupSetName;
	@Column(name = "DESCRIPTION")
	private String description;
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

	public long getLookupsetId() {
		return lookupsetId;
	}

	public void setLookupsetId(long lookupsetId) {
		this.lookupsetId = lookupsetId;
	}

	public String getLookupSetName() {
		return lookupSetName;
	}

	public void setLookupSetName(String lookupSetName) {
		this.lookupSetName = lookupSetName;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

}
