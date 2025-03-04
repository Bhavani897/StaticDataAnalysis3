package com.rite.products.convertrite.po;

import lombok.*;
@Data
@NoArgsConstructor
public class CrMappingSetValuesCreateReqPo {
	
	public Long mapLineId;

	public Long mapSetId;

	public String sourceField1;

	public String sourceField2;

	public String sourceField3;

	public String targetValue;

	public String enabledFlag;

	public String attribute1;

	public String attribute2;

	public String attribute3;

	public String attribute4;

	public String attribute5;

	public String insertOrDelete;

}
