package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "xxr_cloud_master_lookup_tab")
public class XxrCloudMasterLookupTab implements Serializable {

	private static final long serialVersionUID = 1L;

	@Id
	@Column(name = "LOOKUP_SET_ID")
	private long lookupSetId;
	@Column(name = "CLOUD_OBJECT")
	private String cloudObject;
	@Column(name = "CLOUD_COLUMN")
	private String cloudColumn;

	public long getLookupSetId() {
		return lookupSetId;
	}

	public void setLookupSetId(long lookupSetId) {
		this.lookupSetId = lookupSetId;
	}

	public String getCloudObject() {
		return cloudObject;
	}

	public void setCloudObject(String cloudObject) {
		this.cloudObject = cloudObject;
	}

	public String getCloudColumn() {
		return cloudColumn;
	}

	public void setCloudColumn(String cloudColumn) {
		this.cloudColumn = cloudColumn;
	}

}
