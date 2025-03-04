package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrLoadDataResPo {
    private long count;
    private String batchNames;
    private String presult;
    private Long duplicateRecCount;
}
