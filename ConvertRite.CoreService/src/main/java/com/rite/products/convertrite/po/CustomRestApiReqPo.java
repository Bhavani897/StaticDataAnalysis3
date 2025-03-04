package com.rite.products.convertrite.po;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Data
public class CustomRestApiReqPo {
    private long cldTemplateId;
    @NotNull(message = "BatchName cannot be null")
    @NotEmpty(message = "BatchName cannot be empty")
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
