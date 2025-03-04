package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrColumnPo {

    public CrColumnPo(long columnId, String columnName, String nullAllowedFlag, String columnType, long width,  long columnSequence) {
        this.columnId = columnId;
        this.columnName = columnName;
        this.nullAllowedFlag = nullAllowedFlag;
        this.columnType = columnType;
        this.columnSequence = columnSequence;
        this.width = width;
    }

    private long columnId;
    private String columnName;
    private String nullAllowedFlag;
    private String columnType;
    private long columnSequence;
    private long width;
}
