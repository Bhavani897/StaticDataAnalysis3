package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CrItemsResPo {
    @JsonProperty("MappingNumber")
    private String mappingNumber;
}
