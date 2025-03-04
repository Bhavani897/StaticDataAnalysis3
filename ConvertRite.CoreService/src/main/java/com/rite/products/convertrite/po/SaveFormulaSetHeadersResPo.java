package com.rite.products.convertrite.po;

public class SaveFormulaSetHeadersResPo {

	private long formulaSetId;
	private String message;
	private String error;

	public long getFormulaSetId() {
		return formulaSetId;
	}

	public void setFormulaSetId(long formulaSetId) {
		this.formulaSetId = formulaSetId;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
