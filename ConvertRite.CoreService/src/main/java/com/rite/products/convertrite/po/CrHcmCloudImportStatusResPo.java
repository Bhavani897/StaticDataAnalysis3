package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrHcmCloudImportStatusResPo {

    private String importLinesTotalCount;
    private String importErrorCount;
    private String errorCount;
    private String loadedCount;

}
