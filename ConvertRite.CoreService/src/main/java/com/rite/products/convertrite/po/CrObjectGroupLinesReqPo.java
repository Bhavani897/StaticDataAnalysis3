package com.rite.products.convertrite.po;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class CrObjectGroupLinesReqPo {

	private Long objGrpLineId;

	private Long groupId;

	private Long objectId;

	private Long sequence;

	private String attribute1;

	private String attribute2;

	private String attribute3;

	private String attribute4;

	private String attribute5;

	private String insertOrDelete;
}
