package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class LoadSourceDataResPo {

	private int loadedRecords;
	private long failedRecords;
	private String stgTableName;
	private String message;
	private String error;
	private String batchNames;
	private String presult;
	private Long duplicateRecCount;
}
