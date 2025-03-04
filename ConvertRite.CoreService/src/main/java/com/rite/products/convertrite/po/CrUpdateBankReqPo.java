package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrUpdateBankReqPo {

    @JsonProperty("BankPartyId")
    private Long bankPartyId;

    @JsonProperty("BankName")
    private String bankName;

    @JsonProperty("BankNameAlt")
    private String bankNameAlt;

    @JsonProperty("BankNumber")
    private String bankNumber;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("TaxpayerIdNumber")
    private String taxpayerIdNumber;

    @JsonProperty("TaxRegistrationNumber")
    private String taxRegistrationNumber;
}
