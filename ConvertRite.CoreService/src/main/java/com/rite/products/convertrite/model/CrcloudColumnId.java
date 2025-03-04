package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Data
@Embeddable
public class CrcloudColumnId implements Serializable {
    private static final long serialVersionUID = 1L;
    @Column(name = "COLUMN_ID")
    private Long columnId;
    @Column(name = "TABLE_ID")
    private Long tableId;
}
