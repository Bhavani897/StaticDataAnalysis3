package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
@Data
public class CrBankAccount {
    @JsonProperty("AccountHolderName")
    private String accountHolderName;

    @JsonProperty("AccountType")
    private String accountType;

    @JsonProperty("ApUseAllowedFlag")
    private String apUseAllowedFlag;

    @JsonProperty("ArUseAllowedFlag")
    private String arUseAllowedFlag;

    @JsonProperty("BankAccountName")
    private String bankAccountName;

    @JsonProperty("BankAccountNumber")
    private String bankAccountNumber;

    @JsonProperty("BankBranchName")
    private String bankBranchName;

    @JsonProperty("BankName")
    private String bankName;

    @JsonProperty("CountryName")
    private String countryName;

    @JsonProperty("CurrencyCode")
    private String currencyCode;

    @JsonProperty("LegalEntityName")
    private String legalEntityName;

    @JsonProperty("MultiCurrencyAllowedFlag")
    private String multiCurrencyAllowedFlag;

    @JsonProperty("NettingAccountFlag")
    private String nettingAccountFlag;

    @JsonProperty("PayUseAllowedFlag")
    private String payUseAllowedFlag;

    @JsonProperty("ZeroAmountAllowed")
    private String zeroAmountAllowed;

    @JsonProperty("bankAccountUses")
    private List<BankAccountUse> bankAccountUses;

}
