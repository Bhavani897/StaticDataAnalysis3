package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CrCreateBankReqPo {
	@JsonProperty("CountryName")
	private String countryName;
	@JsonProperty("BankName")
	private String bankName;
	@JsonProperty("BankNumber")
	private String bankNumber;
}
