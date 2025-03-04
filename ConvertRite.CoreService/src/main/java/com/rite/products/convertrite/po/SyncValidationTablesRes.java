package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class SyncValidationTablesRes {
    private String errorMessage;
    private boolean isValidationTableSyncFailed;
}
