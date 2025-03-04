package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CrUpdateProjectDffReqPo {
    @JsonProperty("parentProjectNumber")
    private String parentProjectId;
    @JsonProperty("parentProjectNumber_Display")
    private String parentProjectNumber;
}
