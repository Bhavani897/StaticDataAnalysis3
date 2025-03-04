package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectInfoWithPodClodConfigPo {

    private Long objectId;
    private String objectName;
    private String objectCode;
    private String userObjectName;
    private String moduleCode;
    private Long parentObjectId;
    private String fbdiSheet;
    private String hdlSheet;
    private String loaderEndpoint;
    private String reConQuery;
    private String sequenceInParent;
    private String insertTableName;
    private String rejectionTableName;
    private String ctlFileName;
    private String xlsmFileName;
    private String immediateParent;
    private Long batchSize;
    private String baseTables;
//    private String creationDate;
//    private String createdBy;
//    private String lastUpdatedDate;
//    private String lastUpdatedBy;
    private List<CrObjectInformation> crObjectInformation;
}
