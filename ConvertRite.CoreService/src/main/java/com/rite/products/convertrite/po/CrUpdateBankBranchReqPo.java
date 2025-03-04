package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CrUpdateBankBranchReqPo {

    @JsonProperty("BankBranchName")
    private String bankBranchName;

    @JsonProperty("BankBranchNameAlt")
    private String bankBranchNameAlt;

    @JsonProperty("BankBranchType")
    private String bankBranchType;

    @JsonProperty("BankName")
    private String bankName;

    @JsonProperty("BranchNumber")
    private String branchNumber;

    @JsonProperty("BranchNumberType")
    private String branchNumberType;

    @JsonProperty("BranchPartyId")
    private Long branchPartyId;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("EDIIdNumber")
    private String ediIdNumber;

    @JsonProperty("EDILocation")
    private String ediLocation;

    @JsonProperty("EFTSWIFTCode")
    private String eftSwiftCode;

    @JsonProperty("EFTUserNumber")
    private String eftUserNumber;

    @JsonProperty("RFCIdentifier")
    private String rfcIdentifier;
}
