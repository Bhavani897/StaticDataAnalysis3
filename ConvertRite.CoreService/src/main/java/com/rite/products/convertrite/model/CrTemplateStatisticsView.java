package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data
@Table(name="CR_TEMPLATE_STATISTICS_V")
public class CrTemplateStatisticsView {
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
}
