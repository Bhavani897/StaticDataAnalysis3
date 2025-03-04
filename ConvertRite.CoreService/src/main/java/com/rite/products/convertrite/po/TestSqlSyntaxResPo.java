package com.rite.products.convertrite.po;

public class TestSqlSyntaxResPo {

	private String message;
	private boolean isValidSyntax;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	
	public boolean isValidSyntax() {
		return isValidSyntax;
	}

	public void setValidSyntax(boolean isValidSyntax) {
		this.isValidSyntax = isValidSyntax;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
