package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CustomRestApiReqPo {
    private long cldTemplateId;
    private String batchName;
    private String restApiUrl;
    private String cldUserName;
    private String cldPassword;
    private String objectName;
    private String cloudUrl;
    private Long objectId;
    private Long podId;
    private Long requestId;
    private String ccidColumnName;
    private String ledgerColumnName;
    private String createdBy;
}
