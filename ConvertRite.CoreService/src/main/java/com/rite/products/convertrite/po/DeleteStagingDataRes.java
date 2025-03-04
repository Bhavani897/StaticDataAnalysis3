package com.rite.products.convertrite.po;

public class DeleteStagingDataRes {

	private int[] countOfRowsDeleted;
	private String message;

	public int[] getCountOfRowsDeleted() {
		return countOfRowsDeleted;
	}

	public void setCountOfRowsDeleted(int[] countOfRowsDeleted) {
		this.countOfRowsDeleted = countOfRowsDeleted;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
