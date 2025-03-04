package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import java.io.Serializable;
import java.sql.Date;


@Entity
@Table(name = "XXR_MAPPING_SETS")
public class XxrMappingSets implements Serializable {

	private static final long serialVersionUID = 1L;

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
	@Column(name = "CLOUD_COLUMN")
	private String cloudColumn;
	@Column(name = "SOURCE_OBJECT1")
	private String sourceObject1;
	@Column(name = "SOURCE_OBJECT2")
	private String sourceObject2;
	@Column(name = "SOURCE_OBJECT3")
	private String sourceObject3;
	@Column(name = "SOURCE_COLUMN1")
	private String sourceColumn1;
	@Column(name = "SOURCE_COLUMN2")
	private String sourceColumn2;
	@Column(name = "SOURCE_COLUMN3")
	private String sourceColumn3;
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
	@Column(name="LOOKUP_SET_NAME")
	private String lookupSetName;
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
	

	public String getLookupSetName() {
		return lookupSetName;
	}

	public void setLookupSetName(String lookupSetName) {
		this.lookupSetName = lookupSetName;
	}

	public String getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(String whereClause) {
		this.whereClause = whereClause;
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

	public String getCloudColumn() {
		return cloudColumn;
	}

	public void setCloudColumn(String cloudColumn) {
		this.cloudColumn = cloudColumn;
	}

	public String getSourceObject1() {
		return sourceObject1;
	}

	public void setSourceObject1(String sourceObject1) {
		this.sourceObject1 = sourceObject1;
	}

	public String getSourceObject2() {
		return sourceObject2;
	}

	public void setSourceObject2(String sourceObject2) {
		this.sourceObject2 = sourceObject2;
	}

	public String getSourceObject3() {
		return sourceObject3;
	}

	public void setSourceObject3(String sourceObject3) {
		this.sourceObject3 = sourceObject3;
	}

	public String getSourceColumn1() {
		return sourceColumn1;
	}

	public void setSourceColumn1(String sourceColumn1) {
		this.sourceColumn1 = sourceColumn1;
	}

	public String getSourceColumn2() {
		return sourceColumn2;
	}

	public void setSourceColumn2(String sourceColumn2) {
		this.sourceColumn2 = sourceColumn2;
	}

	public String getSourceColumn3() {
		return sourceColumn3;
	}

	public void setSourceColumn3(String sourceColumn3) {
		this.sourceColumn3 = sourceColumn3;
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
}
