package com.rite.products.convertrite.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Entity
@Table(name = "CR_FORMULA_SETS")
public class CrFormulaSets {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "FORMULA_SET_ID")
	public Long formulaSetId;
	
	@Column(name= "FORMULA_SET_NAME")
	public String formulaSetName;
	
	@Column(name= "FORMULA_SET_CODE")
	public String formulaSetCode;
	
	@Column(name = "DESCRIPTION")
	private String description;
	
	@Column(name= "FORMULA_TYPE")
	public String formulaType;
	
	@Column(name= "FORMULA_TEXT")
	public String formulaText;
	
	@Column(name= "COUNT_OF_PARAMS")
	public Long countOfParams;
	
	@Column(name = "ATTRIBUTE1")
	private String attribute1;

	@Column(name = "ATTRIBUTE2")
	private String attribute2;

	@Column(name = "ATTRIBUTE3")
	private String attribute3;

	@Column(name = "ATTRIBUTE4")
	private String attribute4;

	@Column(name = "ATTRIBUTE5")
	private String attribute5;
		
	@Column(name = "CREATION_DATE")
	private Date creationDate;

	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;

	@Column(name = "CREATED_BY")
	private String createdBy;

	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy;
}
