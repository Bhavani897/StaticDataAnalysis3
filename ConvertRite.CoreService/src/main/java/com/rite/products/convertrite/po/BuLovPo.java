package com.rite.products.convertrite.po;

public class BuLovPo {

	private long buId;
	private String buName;
	
	

	public BuLovPo(long buId, String buName) {
		super();
		this.buId = buId;
		this.buName = buName;
	}

	public long getBuId() {
		return buId;
	}

	public void setBuId(long buId) {
		this.buId = buId;
	}

	public String getBuName() {
		return buName;
	}

	public void setBuName(String buName) {
		this.buName = buName;
	}

}
