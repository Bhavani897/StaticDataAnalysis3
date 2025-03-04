package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrObjectInformation {
    private Long objInfoId;
    private Long objectId;
    private String infoType;
    private String infoValue;
    private String infoDescription;
    private String additionalInformation1;
    private String additionalInformation2;
    private String additionalInformation3;
    private String additionalInformation4;
    private String additionalInformation5;
    private String creationDate;
    private String createdBy;
    private String lastUpdateDate;
    private String lastUpdateBy;
}
