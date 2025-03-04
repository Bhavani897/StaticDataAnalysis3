package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_LOOKUP_SETS")
public class XxrLookupSets {

	
	@Column(name = "LOOKUP_SET_ID")
	private Long lookUpSetId;
	@Id
	@Column(name = "LOOKUP_SET_NAME")
	private String lookUpSetName;
	@Column(name = "RELATED_TO")
	private String relatedTo;
	@Column(name = "DESCRIPTION")
	private String description;
	@Column(name="LOOKUP_FLAG")
	private String lookupFlag;
	
	
	public String getLookupFlag() {
		return lookupFlag;
	}

	public void setLookupFlag(String lookupFlag) {
		this.lookupFlag = lookupFlag;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getLookUpSetId() {
		return lookUpSetId;
	}

	public void setLookUpSetId(Long lookUpSetId) {
		this.lookUpSetId = lookUpSetId;
	}

	public String getLookUpSetName() {
		return lookUpSetName;
	}

	public void setLookUpSetName(String lookUpSetName) {
		this.lookUpSetName = lookUpSetName;
	}

	public String getRelatedTo() {
		return relatedTo;
	}

	public void setRelatedTo(String relatedTo) {
		this.relatedTo = relatedTo;
	}

	/*
	 * @Column(name="DESCRIPTION") private String description;
	 * 
	 * @Column(name="ATTRIBUTE1") private String attribute1;
	 * 
	 * @Column(name="ATTRIBUTE2") private String attribute2;
	 * 
	 * @Column(name="ATTRIBUTE3") private String attribute3;
	 * 
	 * @Column(name="ATTRIBUTE4") private String attribute4;
	 * 
	 * @Column(name="ATTRIBUTE5") private String attribute5;
	 * 
	 * @Column(name="LAST_UPDATED_BY") private String lastUpdateBy;
	 * 
	 * @Column(name="CREATED_BY") private String createdBy;
	 */

}
