package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class ReOrigTransRefReqPo {
    private Long srcTemplateId;
    private String srcStgTableName;
    private String batchName;
}