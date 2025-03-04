package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;

public class XxrFormulaColumnsId implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	@Column(name = "FORMULA_SET_ID")
	private long formulaSetId;
	
	@Column(name = "FORMULA_TABLE_ID")
	private long formulaTableId;
	
	@Column(name = "SOURCE_COLUMN_ID")
	private long sourceColumnId;

	public long getFormulaSetId() {
		return formulaSetId;
	}

	public void setFormulaSetId(long formulaSetId) {
		this.formulaSetId = formulaSetId;
	}

	public long getFormulaTableId() {
		return formulaTableId;
	}

	public void setFormulaTableId(long formulaTableId) {
		this.formulaTableId = formulaTableId;
	}

	public long getSourceColumnId() {
		return sourceColumnId;
	}

	public void setSourceColumnId(long sourceColumnId) {
		this.sourceColumnId = sourceColumnId;
	}
	
	
	
}
