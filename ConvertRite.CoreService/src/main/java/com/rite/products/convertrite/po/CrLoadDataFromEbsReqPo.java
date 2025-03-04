package com.rite.products.convertrite.po;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Data
public class CrLoadDataFromEbsReqPo {
	@NotEmpty(message="connectionName cannot be empty")
	private String connectionName;
	@NotNull(message="srcTemplateId cannot be null")
	private Long srcTemplateId;
	@NotEmpty(message="batchName cannot be empty")
	private String batchName;
	@NotNull(message="batchSize cannot be null")
	private Long batchSize;
	private String parentStgTableName;
	private String parentColumn;
}
