package com.rite.products.convertrite.po;

import lombok.Data;

import java.util.Date;
@Data
public class CrCloudReqPo {
    private String sqlQuery;
    private String destinationType;
    private String lastUpdatedBy;
    private Date lastUpdateDate;
    private Date creationDate;
    private String createdBy;
    private String lookUpFlag;
    private String scheduledJobCall;
    private Integer podId;
    private String tableName;
    private String metaData;
    private Boolean isInternalServiceCall;
    private Integer batchSize;
    private String apiCallFrom;
    private String dataSync;

}