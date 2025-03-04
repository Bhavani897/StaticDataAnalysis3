package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name="CR_SRC_TEMPLATE_COLS")
public class CrSourceTemplateColumns {
    @Id
    @Column(name = "COLUMN_ID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long columnId;

    @Column(name = "SRC_TEMPLATE_ID")
    private long templateId;

    @Column(name = "COLUMN_NAME")
    private String columnName;

    @Column(name = "SELECTED")
    private String selected;
    @Column(name = "UNIQUE_TRANS_REF")
    private String uniqueTransRef;

    @Column(name = "DISPLAY_SEQ")
    private Integer seq;
    @Column(name = "COLUMN_TYPE")
    private String columnType;
    @Column(name = "WIDTH")
    private Long width;

    @Column(name = "ATTRIBUTE1")
    private String attribute1;
    @Column(name = "ATTRIBUTE2")
    private String attribute2;
    @Column(name = "ATTRIBUTE3")
    private String attribute3;
    @Column(name = "ATTRIBUTE4")
    private String attribute4;
    @Column(name = "ATTRIBUTE5")
    private String attribute5;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;

}
