package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Entity
@Table(name="XXR_TEMPLATE_STATISTICS_V")
public class XxrTemplateStatisticsView {
	
	@Id
	@Column(name="CRITERIA_ID")
	private Long criteriaId;
	@Column(name="CRITERIA_NAME")
	private String criteriaName;
	@JsonIgnore
	@Column(name="CRITERIA_TYPE")
	private String criteriaType;
	@Column(name="SUCCESS")
	private Integer success;
	@Column(name="FAILED")
	private Integer failed;
	@Column(name="UNVERIFIED")
	private Integer unverified;
	public Long getCriteriaId() {
		return criteriaId;
	}
	public void setCriteriaId(Long criteriaId) {
		this.criteriaId = criteriaId;
	}
	public String getCriteriaName() {
		return criteriaName;
	}
	public void setCriteriaName(String criteriaName) {
		this.criteriaName = criteriaName;
	}
	public String getCriteriaType() {
		return criteriaType;
	}
	public void setCriteriaType(String criteriaType) {
		this.criteriaType = criteriaType;
	}
	public Integer getSuccess() {
		return success;
	}
	public void setSuccess(Integer success) {
		this.success = success;
	}
	public Integer getFailed() {
		return failed;
	}
	public void setFailed(Integer failed) {
		this.failed = failed;
	}
	public Integer getUnverified() {
		return unverified;
	}
	public void setUnverified(Integer unverified) {
		this.unverified = unverified;
	}
	
	
	

}
