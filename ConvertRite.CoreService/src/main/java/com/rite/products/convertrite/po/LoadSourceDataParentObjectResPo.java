package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class LoadSourceDataParentObjectResPo {
	private Long srcTemplateId;
	private String srcTemplateName;
	private int loadedRecords;
	private long failedRecords;
	private String message;
	private String error;
	private String  stgTableName;

}
