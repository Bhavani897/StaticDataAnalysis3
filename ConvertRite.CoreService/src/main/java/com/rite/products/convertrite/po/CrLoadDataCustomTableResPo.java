package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrLoadDataCustomTableResPo {

    private int loadedRecords;
    private long failedRecords;
    private String crBatchName;
    private String customTableName;
}
