package com.rite.products.convertrite.po;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
public class LoadCustomDataReqPo {
    @NotNull(message = "customTableId cannot be null")
    private Long customTableId;
    @NotBlank(message = "crBatchName cannot be blank")
    private String crBatchName;
    @NotBlank(message="fileName cannot be blank")
    private String fileName;
}
