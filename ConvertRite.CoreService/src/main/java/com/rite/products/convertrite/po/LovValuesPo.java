package com.rite.products.convertrite.po;

public class LovValuesPo {
	
	private String value;
	
	private long id;
	private String lable;
	public String getLable() {
		return lable;
	}

	public void setLable(String lable) {
		this.lable = lable;
	}

	public LovValuesPo(String value, long id, String lable) {
		super();
		this.value = value;
		this.id = id;
		this.lable=lable;
	}
	public LovValuesPo(String value, long id) {
		super();
		this.value = value;
		this.id = id;
	}
	
	public LovValuesPo(long id,String value) {
		super();
		this.value = value;
		this.id = id;
	}

	public String getValue() {
		return value;
	}

	public long getId() {
		return id;
	}
	
	
	

}
