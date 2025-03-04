package com.rite.products.convertrite.po;

public class CloudMappingSetPo {

	private long mapId;
	private String name;
	private String type;
	private String cloudColumnName;
	private String SourceColumnName;
	
	public CloudMappingSetPo(long mapId, String name, String type){//, String cloudColumnName, String sourceColumnName) {
		super();
		this.mapId = mapId;
		this.name = name;
		this.type = type;
		//this.cloudColumnName = cloudColumnName;
		//SourceColumnName = sourceColumnName;
	}

	public long getMapId() {
		return mapId;
	}

	public void setMapId(long mapId) {
		this.mapId = mapId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getCloudColumnName() {
		return cloudColumnName;
	}

	public void setCloudColumnName(String cloudColumnName) {
		this.cloudColumnName = cloudColumnName;
	}

	public String getSourceColumnName() {
		return SourceColumnName;
	}

	public void setSourceColumnName(String sourceColumnName) {
		SourceColumnName = sourceColumnName;
	}
	
	
	
}
