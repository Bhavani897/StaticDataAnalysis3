package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrFormulaSetsCreateReqPo {

	public Long formulaSetId;

	public String formulaSetName;

	public String formulaSetCode;

	private String description;

	public String formulaType;

	public String formulaText;
	
	public Long countOfParams;

	private String attribute1;

	private String attribute2;

	private String attribute3;

	private String attribute4;

	private String attribute5;
}
