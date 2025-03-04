package com.rite.products.convertrite.po;

import java.util.List;

public class DeleteStagingDataReq {

	private List<String> srcStagingTables;
	private List<String> cldStagingTables;

	public List<String> getSrcStagingTables() {
		return srcStagingTables;
	}

	public void setSrcStagingTables(List<String> srcStagingTables) {
		this.srcStagingTables = srcStagingTables;
	}

	public List<String> getCldStagingTables() {
		return cldStagingTables;
	}

	public void setCldStagingTables(List<String> cldStagingTables) {
		this.cldStagingTables = cldStagingTables;
	}

}
