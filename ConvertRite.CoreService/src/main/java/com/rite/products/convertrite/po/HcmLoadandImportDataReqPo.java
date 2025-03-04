package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class HcmLoadandImportDataReqPo {

	private Long podId;
	private Long projectId;
	private Integer parentObjectId;
	private String cloudTemplateId;
	private String documentTitle;
	private String documentAuthor;
	private String documentSecurityGroup;
	private String documentAccount;
	private String batchName;
	private String isIntialLoad;
}
