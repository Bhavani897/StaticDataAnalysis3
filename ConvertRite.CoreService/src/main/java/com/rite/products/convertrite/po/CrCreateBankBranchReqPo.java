package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CrCreateBankBranchReqPo {

    @JsonProperty("BankName")
    private String bankName;
    @JsonProperty("BankBranchName")
    private String bankBranchName;
    @JsonProperty("CountryName")
    private String countryName;
    @JsonProperty("BranchNumber")
    private String branchNumber;
    @JsonProperty("EFTSWIFTCode")
    private String eftSwiftCode;

}
