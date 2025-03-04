package com.rite.products.convertrite.exception;

import org.springframework.http.HttpStatus;

public class ConvertRiteException extends Exception{
	
	private static final long serialVersionUID = 1L;
	
	private String errorMessage;
	private HttpStatus errorCode;
	
	public ConvertRiteException(String errorMessage, HttpStatus errorCode) {
		
		this.errorMessage = errorMessage;
		this.errorCode = errorCode;
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	public HttpStatus getErrorCode() {
		return errorCode;
	}
	public void setErrorCode(HttpStatus errorCode) {
		this.errorCode = errorCode;
	}

	
	
}
