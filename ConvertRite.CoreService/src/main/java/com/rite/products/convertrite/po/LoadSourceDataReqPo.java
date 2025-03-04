package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class LoadSourceDataReqPo {
	private Long metaDataTableId;
	private String srcStgTableName;
	private Long srcTemplateId;
	private String fileName;
	private String batchName;
	private Long batchSize;
	private String parentStgTableName;
	private String parentColumn;
}
