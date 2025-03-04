package com.rite.products.convertrite.po;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class CrCloudTemplateColumnsReqPo {

	private long columnId;
	private String columnName;
	private long templateId;
	private String description;
	private String columnType;
	private Long width;
	private String nullAllowedFlag;
	private String uniqueTransRef;
	private String selected;
	private Long sourceColumnId;
	private String sourceColumnName;
	private String mappingType;
	private Long mappingSetId;
	private String mappingSetName;
	private String mappingValue1;
	private String mappingValue2;
	private String mappingValue3;
	private String mappingValue4;
	private String mappingValue5;
	private Integer seq;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;
	private String insertOrDelete;
}
