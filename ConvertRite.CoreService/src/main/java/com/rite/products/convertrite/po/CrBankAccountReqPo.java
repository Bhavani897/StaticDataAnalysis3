package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrBankAccountReqPo {

    @JsonProperty("AccountHolderName")
    private String accountHolderName;

    @JsonProperty("AccountHolderNameAlt")
    private String accountHolderNameAlt;

    @JsonProperty("AccountingConversionRateType")
    private String accountingConversionRateType;

    @JsonProperty("AccountSuffix")
    private String accountSuffix;

    @JsonProperty("AccountType")
    private String accountType;

    @JsonProperty("AgencyLocationCode")
    private String agencyLocationCode;

    @JsonProperty("ApUseAllowedFlag")
    private Boolean apUseAllowedFlag;

    @JsonProperty("ArUseAllowedFlag")
    private Boolean arUseAllowedFlag;

    @JsonProperty("BankAccountId")
    private Long bankAccountId;

    @JsonProperty("BankAccountName")
    private String bankAccountName;

    @JsonProperty("BankAccountNameAlt")
    private String bankAccountNameAlt;

    @JsonProperty("BankAccountNumber")
    private String bankAccountNumber;

    @JsonProperty("BankAccountNumberElectronic")
    private String bankAccountNumberElectronic;

    @JsonProperty("BankBranchName")
    private String bankBranchName;

    @JsonProperty("BankExchangeRateType")
    private String bankExchangeRateType;

    @JsonProperty("BankName")
    private String bankName;

    @JsonProperty("BranchNumber")
    private String branchNumber;

    @JsonProperty("CashAccountCombination")
    private String cashAccountCombination;

    @JsonProperty("CashCcidFixedSegments")
    private String cashCcidFixedSegments;

    @JsonProperty("CashClearingAccountCombination")
    private String cashClearingAccountCombination;

    @JsonProperty("CheckDigits")
    private String checkDigits;

    @JsonProperty("CountryName")
    private String countryName;

    @JsonProperty("CurrencyCode")
    private String currencyCode;

    @JsonProperty("DataSecurityFlag")
    private Boolean dataSecurityFlag;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("EFTUserNumber")
    private String eFTUserNumber;

    @JsonProperty("EndDate")
    private String endDate;

    @JsonProperty("GlReconStartDate")
    private String glReconStartDate;

    @JsonProperty("IBANNumber")
    private String ibanNumber;

    @JsonProperty("LegalEntityName")
    private String legalEntityName;

    @JsonProperty("MaskedAccountNumber")
    private String maskedAccountNumber;

    @JsonProperty("MaskedIBAN")
    private String maskedIBAN;

    @JsonProperty("MaximumCheckAmount")
    private Double maximumCheckAmount;

    @JsonProperty("MaximumOutlay")
    private Double maximumOutlay;

    @JsonProperty("MinimumCheckAmount")
    private Double minimumCheckAmount;

    @JsonProperty("MultiCashReconEnabledFlag")
    private Boolean multiCashReconEnabledFlag;

    @JsonProperty("MultiCurrencyAllowedFlag")
    private Boolean multiCurrencyAllowedFlag;

    @JsonProperty("NettingAccountFlag")
    private Boolean nettingAccountFlag;

    @JsonProperty("ParsingRuleSetName")
    private String parsingRuleSetName;

    @JsonProperty("PayUseAllowedFlag")
    private Boolean payUseAllowedFlag;

    @JsonProperty("PooledFlag")
    private Boolean pooledFlag;

    @JsonProperty("ReconciliationDifferenceAccountCombination")
    private String reconciliationDifferenceAccountCombination;

    @JsonProperty("ReconStartDate")
    private String reconStartDate;

    @JsonProperty("ReversalProcessingMethod")
    private String reversalProcessingMethod;

    @JsonProperty("RulesetName")
    private String rulesetName;

    @JsonProperty("SecondaryAccountReference")
    private String secondaryAccountReference;

    @JsonProperty("TargetBalance")
    private Double targetBalance;

    @JsonProperty("ToleranceRuleName")
    private String toleranceRuleName;

    @JsonProperty("TransactionCalendar")
    private String transactionCalendar;

    @JsonProperty("ZeroAmountAllowed")
    private String zeroAmountAllowed;
    @JsonProperty("bankAccountUses")
    private List<BankAccountUse> bankAccountUses;
}
