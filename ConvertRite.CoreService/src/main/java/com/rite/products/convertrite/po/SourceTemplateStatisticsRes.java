package com.rite.products.convertrite.po;

import java.util.List;

public class SourceTemplateStatisticsRes {

	private String criteriaType;

	private List<SourceTemplateStatisticsResPo> data;

	public String getCriteriaType() {
		return criteriaType;
	}

	public void setCriteriaType(String criteriaType) {
		this.criteriaType = criteriaType;
	}

	public List<SourceTemplateStatisticsResPo> getData() {
		return data;
	}

	public void setData(List<SourceTemplateStatisticsResPo> data) {
		this.data = data;
	}

}
