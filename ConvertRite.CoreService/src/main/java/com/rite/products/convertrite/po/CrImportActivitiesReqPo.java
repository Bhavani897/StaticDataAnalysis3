package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class CrImportActivitiesReqPo {
    @JsonProperty("Name")
    private String name;
    @JsonProperty("ObjectCode")
    private String objectCode;
    @JsonProperty("ImportMode")
    private String importMode;
    @JsonProperty("ImportMapping")
    private String importMapping;
    @JsonProperty("HighVolume")
    private String highVolume;
    @JsonProperty("Activate")
    private String activate;
    @JsonProperty("DataFiles")
    private List<CrDataFilesReqPo> dataFiles;
}
