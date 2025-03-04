package com.rite.products.convertrite.po;

import lombok.Data;

import java.util.List;

@Data
public class ExecuteCustomObjectReq {
    private List<Long> objectIds;
    private Long projectId;
    private Long podId;
}
