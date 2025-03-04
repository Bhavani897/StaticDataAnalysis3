package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.rite.products.convertrite.model.CloudLoginDetails;
import com.rite.products.convertrite.model.ObjectInfoWithPodClodConfigPo;
import lombok.Data;

import java.util.List;

@Data
public class CrObjectInformationPo {

    @JsonProperty("objectsDetails")
    private List<ObjectInfoWithPodClodConfigPo> objectInfoWithPodClodConfigPos;
    @JsonProperty("cloudLoginDetails")
    private List<CloudLoginDetails> cloudLoginDetails;

}
