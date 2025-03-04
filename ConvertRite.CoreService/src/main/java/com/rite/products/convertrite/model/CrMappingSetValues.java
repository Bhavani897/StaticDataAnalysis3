package com.rite.products.convertrite.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;

@Data
@Entity
@Table(name = "CR_MAPPING_VALUES")
public class CrMappingSetValues {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "MAP_LINE_ID", columnDefinition = "serial")
	public Long mapLineId;

	
	@Column(name = "MAP_SET_ID")
	public Long mapSetId;
	
	
	@Column(name = "SOURCE_FIELD1")
	public String sourceField1;

	@Column(name = "SOURCE_FIELD2")
	public String sourceField2;

	@Column(name = "SOURCE_FIELD3")
	public String sourceField3;

	@Column(name = "TARGET_VALUE")
	public String targetValue;

	@Column(name = "ENABLED_FLAG")
	public String enabledFlag;

	@Column(name = "ATTRIBUTE1")
	public String attribute1;

	@Column(name = "ATTRIBUTE2")
	public String attribute2;

	@Column(name = "ATTRIBUTE3")
	public String attribute3;

	@Column(name = "ATTRIBUTE4")
	public String attribute4;

	@Column(name = "ATTRIBUTE5")
	public String attribute5;

	@Column(name = "CREATION_DATE")
	public Date creationDate;
	
	@Column(name = "CREATED_BY")
	public String createdBy;
	
	@Column(name = "LAST_UPDATE_DATE")
	public Date lastUpdateDate;
	
	@Column(name = "LAST_UPDATED_BY")
	public String lastUpdateBy;
}
