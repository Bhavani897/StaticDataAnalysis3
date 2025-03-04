package com.rite.products.convertrite.po;

import lombok.Data;
import lombok.Getter;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

@Data
@Getter
@NotNull
public class CrCloudRecordsReqPo {
    @NotNull
    private String cloudTemplateName;
    @NotNull
    private String responseType;
    @NotNull
    private String batchName;
    @Positive
    private int pageNo = 1;
    @Positive
    private int pageSize = 50;
}