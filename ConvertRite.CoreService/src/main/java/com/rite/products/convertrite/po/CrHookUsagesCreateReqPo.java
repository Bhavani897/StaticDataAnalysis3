package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrHookUsagesCreateReqPo {

	private Long hookUsageId;

	private Long hookId;

	private Long templateId;

	private String usageType;

	private String attribute1;

	private String attribute2;

	private String attribute3;

	private String attribute4;

	private String attribute5;
	
	private String insertOrDelete;
}
