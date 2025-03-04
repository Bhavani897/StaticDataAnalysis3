package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrBanksResPo {

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

    @JsonProperty("CountryName")
    private String countryName;
}
