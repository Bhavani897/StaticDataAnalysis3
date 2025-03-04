package com.rite.products.convertrite.po;

import lombok.Data;

import java.util.List;

@Data
public class ProcessValidationObjectsReqPo {
    private List<Long> objectIdsLi;
    private Long projectId;
}
