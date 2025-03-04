package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrObjectGroupHdrReqPo {

	private Long groupId;

	private Long projectId;

	private Long parentObjectId;

	private String groupName;

	private String groupCode;

	private String description;

	private String attribute1;

	private String attribute2;

	private String attribute3;

	private String attribute4;

	private String attribute5;
}
