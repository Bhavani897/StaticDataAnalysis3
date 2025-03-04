package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CrSupplierTaxProfileReqPo {
    @JsonProperty("AllowOffsetTaxFlag")
    public boolean allowOffsetTaxFlag;
    @JsonProperty("RegistrationTypeCode")
    public String registrationTypeCode;
}
