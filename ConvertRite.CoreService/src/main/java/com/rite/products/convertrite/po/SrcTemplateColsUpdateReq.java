package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class SrcTemplateColsUpdateReq {

	private Long templateId;
	private String columnName;
	private String columnType;
	private String operationType;
	private Long displaySeq;

}
