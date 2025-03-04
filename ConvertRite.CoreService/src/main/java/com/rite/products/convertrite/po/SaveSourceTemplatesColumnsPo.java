package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class SaveSourceTemplatesColumnsPo {

	private Long templateId;
	private Long columnId;
	private String columnName;
	private String origTransRef;
	//private String mappingType;
	private Integer displaySeq;

	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;
	private String selected;
	private String columnType;
	private Long width;
	public String getColumnType;
}
