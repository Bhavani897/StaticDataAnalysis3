package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "xxr_cloud_mas_lookup_val_tab")
public class XxrCloudMasterLookupValueTab implements Serializable {

	private static final long serialVersionUID = 1L;
	
	@Id
	@Column(name = "LOOKUP_SET_ID")
	private long lookupSetId;
	@Column(name = "LOOKUP_VALUE")
	private String lookupValue;

	public long getLookupSetId() {
		return lookupSetId;
	}

	public void setLookupSetId(long lookupSetId) {
		this.lookupSetId = lookupSetId;
	}

	public String getLookupValue() {
		return lookupValue;
	}

	public void setLookupValue(String lookupValue) {
		this.lookupValue = lookupValue;
	}

}
