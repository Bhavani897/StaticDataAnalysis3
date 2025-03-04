package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.sql.Date;

@Data
@Entity
@Table(name = "CR_CREATE_BANK_ACCOUNT_ERRORS")
public class CrBankAccountErrors {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID", columnDefinition = "serial")
    private Long id;

    @Column(name = "ACCOUNT_HOLDER_NAME")
    private String accountHolderName;

    @Column(name = "ACCOUNT_HOLDER_NAME_ALT")
    private String accountHolderNameAlt;

    @Column(name = "ACCOUNTING_CONVERSION_RATE_TYPE")
    private String accountingConversionRateType;

    @Column(name = "ACCOUNT_SUFFIX")
    private String accountSuffix;

    @Column(name = "ACCOUNT_TYPE")
    private String accountType;

    @Column(name = "AGENCY_LOCATION_CODE")
    private String agencyLocationCode;

    @Column(name = "AP_USE_ALLOWED_FLAG")
    private String apUseAllowedFlag;

    @Column(name = "AR_USE_ALLOWED_FLAG")
    private String arUseAllowedFlag;

    @Column(name = "BANK_ACCOUNT_ID")
    private String bankAccountId;

    @Column(name = "BANK_ACCOUNT_NAME")
    private String bankAccountName;

    @Column(name = "BANK_ACCOUNT_NAME_ALT")
    private String bankAccountNameAlt;

    @Column(name = "BANK_ACCOUNT_NUMBER")
    private String bankAccountNumber;

    @Column(name = "BANK_ACCOUNT_NUMBER_ELECTRONIC")
    private String bankAccountNumberElectronic;

    @Column(name = "BANK_BRANCH_NAME")
    private String bankBranchName;

    @Column(name = "BANK_EXCHANGE_RATE_TYPE")
    private String bankExchangeRateType;

    @Column(name = "BANK_NAME")
    private String bankName;

    @Column(name = "BRANCH_NUMBER")
    private String branchNumber;

    @Column(name = "CASH_ACCOUNT_COMBINATION")
    private String cashAccountCombination;

    @Column(name = "CASH_CCID_FIXED_SEGMENTS")
    private String cashCcidFixedSegments;

    @Column(name = "CASH_CLEARING_ACCOUNT_COMBINATION")
    private String cashClearingAccountCombination;

    @Column(name = "CHECK_DIGITS")
    private String checkDigits;

    @Column(name = "COUNTRY_NAME")
    private String countryName;

    @Column(name = "CURRENCY_CODE")
    private String currencyCode;

    @Column(name = "DATA_SECURITY_FLAG")
    private String dataSecurityFlag;

    @Column(name = "DESCRIPTION")
    private String description;

    @Column(name = "EFT_USER_NUMBER")
    private String eFTUserNumber;

    @Column(name = "END_DATE")
    private String endDate;

    @Column(name = "GL_RECON_START_DATE")
    private String glReconStartDate;

    @Column(name = "IBAN_NUMBER")
    private String ibanNumber;

    @Column(name = "LEGAL_ENTITY_NAME")
    private String legalEntityName;

    @Column(name = "MASKED_ACCOUNT_NUMBER")
    private String maskedAccountNumber;

    @Column(name = "MASKED_IBAN")
    private String maskedIBAN;

    @Column(name = "MAXIMUM_CHECK_AMOUNT")
    private String maximumCheckAmount;

    @Column(name = "MAXIMUM_OUTLAY")
    private String maximumOutlay;

    @Column(name = "MINIMUM_CHECK_AMOUNT")
    private String minimumCheckAmount;

    @Column(name = "MULTI_CASH_RECON_ENABLED_FLAG")
    private String multiCashReconEnabledFlag;

    @Column(name = "MULTI_CURRENCY_ALLOWED_FLAG")
    private String multiCurrencyAllowedFlag;

    @Column(name = "NETTING_ACCOUNT_FLAG")
    private String nettingAccountFlag;

    @Column(name = "PARSING_RULE_SET_NAME")
    private String parsingRuleSetName;

    @Column(name = "PAY_USE_ALLOWED_FLAG")
    private String payUseAllowedFlag;

    @Column(name = "POOLED_FLAG")
    private String pooledFlag;

    @Column(name = "RECONCILIATION_DIFFERENCE_ACCOUNT_COMBINATION")
    private String reconciliationDifferenceAccountCombination;

    @Column(name = "RECON_START_DATE")
    private String reconStartDate;

    @Column(name = "REVERSAL_PROCESSING_METHOD")
    private String reversalProcessingMethod;

    @Column(name = "RULESET_NAME")
    private String rulesetName;

    @Column(name = "SECONDARY_ACCOUNT_REFERENCE")
    private String secondaryAccountReference;

    @Column(name = "TARGET_BALANCE")
    private String targetBalance;

    @Column(name = "TOLERANCE_RULE_NAME")
    private String toleranceRuleName;

    @Column(name = "TRANSACTION_CALENDAR")
    private String transactionCalendar;

    @Column(name = "ZERO_AMOUNT_ALLOWED")
    private String zeroAmountAllowed;

    @Column(name = "ERROR_TYPE")
    private String errorType;
    @Column(name = "CR_BATCH_NAME")
    private String crBatchName;
    @Column(name = "STATUS")
    private String status;
    @Column(name = "CLOUD_TEMPLATE_ID")
    private Long cldTempId;
    @Column(name = "ERROR_MESSAGE")
    private String errorMessage;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
}

