package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrBranchesResPo {

    @JsonProperty("BranchPartyId")
    private Long branchPartyId;

    @JsonProperty("BankName")
    private String bankName;

    @JsonProperty("BankNameAlt")
    private String bankNameAlt;

    @JsonProperty("BankNumber")
    private String bankNumber;

    @JsonProperty("BankBranchName")
    private String bankBranchName;

    @JsonProperty("BankBranchNameAlt")
    private String bankBranchNameAlt;

    @JsonProperty("BranchNumber")
    private String branchNumber;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("EFTSWIFTCode")
    private String eftSwiftCode;

    @JsonProperty("EFTUserNumber")
    private String eftUserNumber;

    @JsonProperty("EDIIdNumber")
    private String ediIdNumber;

    @JsonProperty("EDILocation")
    private String ediLocation;

    @JsonProperty("CreatedBy")
    private String createdBy;

    @JsonProperty("CreationDate")
    private String creationDate;

    @JsonProperty("LastUpdateDate")
    private String lastUpdateDate;

    @JsonProperty("LastUpdateLogin")
    private String lastUpdateLogin;

    @JsonProperty("LastUpdatedBy")
    private String lastUpdatedBy;

    @JsonProperty("BankPartyNumber")
    private String bankPartyNumber;

    @JsonProperty("BranchPartyNumber")
    private String branchPartyNumber;

    @JsonProperty("CountryName")
    private String countryName;

    @JsonProperty("RFCIdentifier")
    private String rfcIdentifier;

    @JsonProperty("BankBranchType")
    private String bankBranchType;

    @JsonProperty("BranchNumberType")
    private String branchNumberType;
}
