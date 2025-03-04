package com.rite.products.convertrite.po;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;

@Data
public class LoadSourceDataReqAtParentObjectPo {

	@NotNull(message="loadSrcTemplates cannot be null")
	private List<Long> loadSrcTemplates;
	@NotNull(message="parentObjectId cannot be null")
	private Long parentObjectId;
	@NotBlank(message="fileName cannot be blank")
	private String fileName;
	@NotBlank(message="batchName cannot be blank")
	private String batchName;
}
