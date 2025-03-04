package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public  class BankAccountUse {
    @JsonProperty("BusinessUnitName")
    private String businessUnitName;
}
