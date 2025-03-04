package com.rite.products.convertrite.po;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Data
public class CrLoadMetaDataFromEbsReqPo {
	
	@NotNull(message="objectId cannot be Null")
	private Long objectId;
	@NotEmpty(message="metaDataTableName cannot be empty")
	private String metaDataTableName;
	@NotEmpty(message="connectionName cannot be empty")
	private String connectionName;

}
