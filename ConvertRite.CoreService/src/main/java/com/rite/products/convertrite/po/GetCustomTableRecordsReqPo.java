package com.rite.products.convertrite.po;

import lombok.Data;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
@Data
@NotNull
public class GetCustomTableRecordsReqPo {
    @NotEmpty(message = "customTableName cannot be empty")
    private String customTableName;
    @NotEmpty(message = "crBatchName cannot be empty")
    private String crBatchName;
    @NotEmpty(message = "responseType cannot be empty")
    private String responseType;
    @Positive
    private Long pageNo;
    @Positive
    private Long pageSize;
}
