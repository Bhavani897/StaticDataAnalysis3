package com.rite.products.convertrite.po;

public class XxrFormulaColumnsResPo {

	private long formulaSetId;
	private long formulaTableId;
	private long formulaColumnId;
	private Long seq;
	private long sourceColumnId;
	private String sourceColumnName;
	private String description;
	private String attribute1;
	private String attribute2;
	private String attribute3;
	private String attribute4;
	private String attribute5;

	public XxrFormulaColumnsResPo(long formulaSetId, long formulaTableId, long formulaColumnId, Long seq,
			long sourceColumnId, String sourceColumnName, String description, String attribute1, String attribute2,
			String attribute3, String attribute4, String attribute5) {
		super();
		this.formulaSetId = formulaSetId;
		this.formulaTableId = formulaTableId;
		this.formulaColumnId = formulaColumnId;
		this.seq = seq;
		this.sourceColumnId = sourceColumnId;
		this.sourceColumnName = sourceColumnName;
		this.description = description;
		this.attribute1 = attribute1;
		this.attribute2 = attribute2;
		this.attribute3 = attribute3;
		this.attribute4 = attribute4;
		this.attribute5 = attribute5;
	}

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

	public String getSourceColumnName() {
		return sourceColumnName;
	}

	public void setSourceColumnName(String sourceColumnName) {
		this.sourceColumnName = sourceColumnName;
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
