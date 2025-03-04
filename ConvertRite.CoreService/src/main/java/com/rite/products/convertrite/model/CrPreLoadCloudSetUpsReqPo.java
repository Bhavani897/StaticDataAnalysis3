package com.rite.products.convertrite.model;

import lombok.Data;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;

@Data
public class CrPreLoadCloudSetUpsReqPo {
    @Size(min = 1, message = "List cannot be empty")
    private List<Long> objectIdLi;
    @NotNull(message="projectId cannot be null")
    private Long projectId;
}
