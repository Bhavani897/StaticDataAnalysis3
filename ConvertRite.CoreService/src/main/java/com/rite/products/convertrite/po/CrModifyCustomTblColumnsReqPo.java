package com.rite.products.convertrite.po;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class CrModifyCustomTblColumnsReqPo {

    @NotNull(message = "customTableId cannot be null")
    private Long customTableId;
    @NotBlank(message = "columnName cannot be blank")
    private String columnName;
    @NotBlank(message = "columnType cannot be blank")
    private String columnType;
    @NotBlank(message = "operationType cannot be blank")
    private String operationType;
    @NotNull(message = "displaySeq cannot be null")
    private Long displaySeq;
}
