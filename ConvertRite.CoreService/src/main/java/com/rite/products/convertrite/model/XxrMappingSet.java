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
@Table(name = "XXR_MAPPING_SET")
public class XxrMappingSet {

	@Column(name = "MAP_SET_ID")
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "xxr_mapping_set_generator")
	@SequenceGenerator(name = "xxr_mapping_set_generator", sequenceName = "xxr_mapping_set_id_s", allocationSize = 1)
	private Long mapSetId;
	@Column(name = "MAP_SET_NAME")
	private String mapSetName;
	@Column(name = "MAP_SET_TYPE")
	private String mapSetType;
	@Column(name = "POD_ID")
	private long podId;
	@Column(name = "PROJECT_ID")
	private long projectId;
	@Column(name = "PARENT_OBJECT_ID")
	private long parentObjectId;
	@Column(name = "OBJECT_ID")
	private long objectId;
	@Column(name = "SOURCE_LOOKUP_SET_ID1")
	private Long sourceLookupSetId1;
	@Column(name = "SOURCE_LOOKUP_SET_ID2")
	private Long sourceLookupSetId2;
	@Column(name = "SOURCE_LOOKUP_SET_ID3")
	private Long sourceLookupSetId3;
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
	@Column(name = "WHERE_CLAUSE")
	private String whereClause;
	@Column(name = "CLOUD_LOOKUP_SET_ID")
	private Long cloudLookupSetId;
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

	public Long getMapSetId() {
		return mapSetId;
	}

	public void setMapSetId(Long mapSetId) {
		this.mapSetId = mapSetId;
	}

	public String getMapSetName() {
		return mapSetName;
	}

	public void setMapSetName(String mapSetName) {
		this.mapSetName = mapSetName;
	}

	public String getMapSetType() {
		return mapSetType;
	}

	public void setMapSetType(String mapSetType) {
		this.mapSetType = mapSetType;
	}

	public long getPodId() {
		return podId;
	}

	public void setPodId(long podId) {
		this.podId = podId;
	}

	public long getProjectId() {
		return projectId;
	}

	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	public long getParentObjectId() {
		return parentObjectId;
	}

	public void setParentObjectId(long parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}

	public Long getSourceLookupSetId1() {
		return sourceLookupSetId1;
	}

	public void setSourceLookupSetId1(Long sourceLookupSetId1) {
		this.sourceLookupSetId1 = sourceLookupSetId1;
	}

	public Long getSourceLookupSetId2() {
		return sourceLookupSetId2;
	}

	public void setSourceLookupSetId2(Long sourceLookupSetId2) {
		this.sourceLookupSetId2 = sourceLookupSetId2;
	}

	public Long getSourceLookupSetId3() {
		return sourceLookupSetId3;
	}

	public void setSourceLookupSetId3(Long sourceLookupSetId3) {
		this.sourceLookupSetId3 = sourceLookupSetId3;
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

	public String getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(String whereClause) {
		this.whereClause = whereClause;
	}

	public Long getCloudLookupSetId() {
		return cloudLookupSetId;
	}

	public void setCloudLookupSetId(Long cloudLookupSetId) {
		this.cloudLookupSetId = cloudLookupSetId;
	}

}
