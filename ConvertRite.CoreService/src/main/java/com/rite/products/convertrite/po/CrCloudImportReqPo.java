package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrCloudImportReqPo {
    private Long cldTemplateId;
    private String batchName;
    private String cldUserName;
    private String cldPassword;
    private Long podId;
    private String cloudUrl;
}
