package com.rite.products.convertrite.model;

import javax.persistence.Column;
import java.io.Serializable;

public class CrCustomColumnId implements Serializable {
    private static final long serialVersionUID = 1L;
    @Column(name="TABLE_ID")
    private Long tableId;
    @Column(name="COLUMN_ID")
    private Integer columnId;
}
