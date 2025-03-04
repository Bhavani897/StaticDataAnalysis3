package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_FORMULA_COLUMNS")
@IdClass(XxrFormulaColumnsId.class)
public class XxrFormulaColumns {
	@Id
	@Column(name = "FORMULA_SET_ID")
	private long formulaSetId;
	@Id
	@Column(name = "FORMULA_TABLE_ID")
	private long formulaTableId;
	@Column(name = "FORMULA_COLUMN_ID")
	private long formulaColumnId;
	@Column(name = "SEQ")
	private Long seq;
	@Id
	@Column(name = "SOURCE_COLUMN_ID")
	private long sourceColumnId;
	@Column(name = "DESCRIPTION")
	private String description;
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

	public long getFormulaColumnId() {
		return formulaColumnId;
	}

	public void setFormulaColumnId(long formulaColumnId) {
		this.formulaColumnId = formulaColumnId;
	}

	public Long getSeq() {
		return seq;
	}

	public void setSeq(Long seq) {
		this.seq = seq;
	}

	public long getSourceColumnId() {
		return sourceColumnId;
	}

	public void setSourceColumnId(long sourceColumnId) {
		this.sourceColumnId = sourceColumnId;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getAttribute1() {
		return attribute1;
	}

	public void setAttribute1(String attribute1) {
		this.attribute1 = attribute1;
	}

	public String getAttribute2() {
		return attribute2;
	}

	public void setAttribute2(String attribute2) {
		this.attribute2 = attribute2;
	}

	public String getAttribute3() {
		return attribute3;
	}

	public void setAttribute3(String attribute3) {
		this.attribute3 = attribute3;
	}

	public String getAttribute4() {
		return attribute4;
	}

	public void setAttribute4(String attribute4) {
		this.attribute4 = attribute4;
	}

	public String getAttribute5() {
		return attribute5;
	}

	public void setAttribute5(String attribute5) {
		this.attribute5 = attribute5;
	}

}
