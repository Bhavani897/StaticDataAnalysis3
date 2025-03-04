package com.rite.products.convertrite.po;

import lombok.Data;

import javax.validation.constraints.Max;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;


@Data
public class CrCreateCustomTableReqPo {

    @NotBlank(message = "customTableName cannot be blank")
    @Size(min = 1, max = 27, message = "Table name must be between 1 and 27 characters")
    private String customTableName;
    @NotNull(message ="projectId cannot be null")
    private Long projectId;
    @NotBlank(message="fileName cannot be blank")
    private String fileName;
    private long parentObjectId;
    private long objectId;

}
