package com.rite.products.convertrite.po;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CrLookupSetValueCreateReqPo {
	
	public Long lookUpValueId;

	public Long lookUpSetId;

	public String lookUpValue;

	public String enabledFlag;

	public String actualValue;

	public String attribute1;

	public String attribute2;

	public String attribute3;

	public String attribute4;

	public String attribute5;
	
	public String insertOrDelete;
}
