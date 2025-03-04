package com.rite.products.convertrite.po;

import java.util.List;

public class CloudTablesTemplatesPo {

	private List<CloudTablesPo> cloudTables;

	private List<TemplatesPo> sourceTemplateHeaders;

	public List<CloudTablesPo> getCloudTables() {
		return cloudTables;
	}

	public void setCloudTables(List<CloudTablesPo> cloudTables) {
		this.cloudTables = cloudTables;
	}

	public List<TemplatesPo> getSourceTemplateHeaders() {
		return sourceTemplateHeaders;
	}

	public void setSourceTemplateHeaders(List<TemplatesPo> sourceTemplateHeaders) {
		this.sourceTemplateHeaders = sourceTemplateHeaders;
	}

}
