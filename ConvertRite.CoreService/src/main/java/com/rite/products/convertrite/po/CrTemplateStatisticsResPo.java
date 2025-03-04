package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.CrTemplateStatisticsView;

public class CrTemplateStatisticsResPo {

	private String criteriaType;
	
	private List<CrTemplateStatisticsView> data;

	public String getCriteriaType() {
		return criteriaType;
	}

	public void setCriteriaType(String criteriaType) {
		this.criteriaType = criteriaType;
	}

	public List<CrTemplateStatisticsView> getData() {
		return data;
	}

	public void setData(List<CrTemplateStatisticsView> data) {
		this.data = data;
	}
}
