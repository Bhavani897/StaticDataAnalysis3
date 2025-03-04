package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class LoadandImportDataReqPo {
	private Long cloudTemplateId;
	private String parameterList;
	private String batchName;
	private String objectName;
	private Long clientId;
	private Long podId;
}
