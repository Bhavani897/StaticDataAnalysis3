package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class ValidationObject {
    private Long validationObjectId;
    private Long objectId;
    private String syncDependentTables;
}
