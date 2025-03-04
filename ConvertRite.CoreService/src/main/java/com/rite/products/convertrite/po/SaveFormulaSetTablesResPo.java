package com.rite.products.convertrite.po;

import java.util.List;

public class SaveFormulaSetTablesResPo {

	private String message;
	private List<XxrFormulaSetTablesResPo> formulaSetTables;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public List<XxrFormulaSetTablesResPo> getFormulaSetTables() {
		return formulaSetTables;
	}

	public void setFormulaSetTables(List<XxrFormulaSetTablesResPo> formulaSetTables) {
		this.formulaSetTables = formulaSetTables;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
