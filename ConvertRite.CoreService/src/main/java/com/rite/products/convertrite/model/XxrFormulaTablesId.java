package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;

public class XxrFormulaTablesId implements Serializable {

	private static final long serialVersionUID = 1L;

	@Column(name = "FORMULA_SET_ID")
	private Long formulaSetId;
	@Column(name = "SOURCE_TABLE_ID")
	private long sourceTableId;

	public Long getFormulaSetId() {
		return formulaSetId;
	}

	public void setFormulaSetId(Long formulaSetId) {
		this.formulaSetId = formulaSetId;
	}

	public long getSourceTableId() {
		return sourceTableId;
	}

	public void setSourceTableId(long sourceTableId) {
		this.sourceTableId = sourceTableId;
	}

}
