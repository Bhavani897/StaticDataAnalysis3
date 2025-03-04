package com.rite.products.convertrite.po;

import java.util.List;

public class SaveFormulaColumnsResPo {
	
	private String message;
	private List<XxrFormulaColumnsResPo> formulaColumns;
	private String error;
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public List<XxrFormulaColumnsResPo> getFormulaColumns() {
		return formulaColumns;
	}
	public void setFormulaColumns(List<XxrFormulaColumnsResPo> formulaColumns) {
		this.formulaColumns = formulaColumns;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
}
