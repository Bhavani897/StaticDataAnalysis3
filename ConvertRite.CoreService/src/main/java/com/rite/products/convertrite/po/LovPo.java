package com.rite.products.convertrite.po;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class LovPo {
	
	/*
	 * @JsonInclude(Include.NON_NULL) private List<LovValuesPo> pod;
	 */
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> bu;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> objectCodes;
	@JsonInclude(Include.NON_NULL)
	private List<LovValuesPo> parentObjectCode;
	
	

	public List<LovValuesPo> getObjectCodes() {
		return objectCodes;
	}
	public void setObjectCodes(List<LovValuesPo> objectCodes) {
		this.objectCodes = objectCodes;
	}
	public List<LovValuesPo> getParentObjectCode() {
		return parentObjectCode;
	}
	public void setParentObjectCode(List<LovValuesPo> parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

	/*
	 * public List<LovValuesPo> getPod() { return pod; } public void
	 * setPod(List<LovValuesPo> pod) { this.pod = pod; }
	 */
	public List<LovValuesPo> getBu() {
		return bu;
	}
	public void setBu(List<LovValuesPo> bu) {
		this.bu = bu;
	}
	
	
	

}
