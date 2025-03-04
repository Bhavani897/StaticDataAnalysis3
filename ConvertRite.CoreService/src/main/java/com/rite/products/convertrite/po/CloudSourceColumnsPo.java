package com.rite.products.convertrite.po;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class CloudSourceColumnsPo {
	@JsonInclude(Include.NON_NULL)
	private List<SourceColumnsPo> sourceColumns;
	private List<ColumnPo> cloudColumns;

	public List<SourceColumnsPo> getSourceColumns() {
		return sourceColumns;
	}

	public void setSourceColumns(List<SourceColumnsPo> sourceColumns) {
		this.sourceColumns = sourceColumns;
	}

	public List<ColumnPo> getCloudColumns() {
		return cloudColumns;
	}

	public void setCloudColumns(List<ColumnPo> cloudColumns) {
		this.cloudColumns = cloudColumns;
	}
}
