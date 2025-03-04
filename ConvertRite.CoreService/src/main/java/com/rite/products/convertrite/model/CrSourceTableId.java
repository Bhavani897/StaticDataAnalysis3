package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;

@Data
public class CrSourceTableId implements Serializable {

    private Long tableId;
    private String tableName;
}
