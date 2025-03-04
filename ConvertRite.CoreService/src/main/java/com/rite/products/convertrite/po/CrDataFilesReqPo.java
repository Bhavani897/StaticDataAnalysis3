package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CrDataFilesReqPo {
    @JsonProperty("InputFileContentId")
    private String inputFileContentId;
}
