package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class CrUpdateProjectDffReqLiPo {
    @JsonProperty("ProjectDFF")
    private List<CrUpdateProjectDffReqPo> projectDff;
}
