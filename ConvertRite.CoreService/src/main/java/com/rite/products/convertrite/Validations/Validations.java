package com.rite.products.convertrite.Validations;

public class Validations {
	
	public static boolean isNullOrEmpty(String string) {
		return string == null || string.isEmpty();
	}
	
	public static boolean isNullOrEmptyorWhiteSpace(String string) {
		return string == null || string.isEmpty() || string.trim().length()==0;
	}
}
