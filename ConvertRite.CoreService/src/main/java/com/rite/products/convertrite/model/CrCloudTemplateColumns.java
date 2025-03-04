package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name="CR_CLD_TEMPLATE_COLS")
public class CrCloudTemplateColumns {
    @Id
    @Column(name = "COLUMN_ID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long columnId;

    @Column(name = "COLUMN_NAME")
    private String columnName;

    @Column(name = "CLD_TEMPLATE_ID")
    private long templateId;

    @Column(name = "DESCRIPTION")
    private String description;

    @Column(name = "COLUMN_TYPE")
    private String columnType;

    @Column(name = "WIDTH")
    private Long width;

    @Column(name = "NULL_ALLOWED_FLAG")
    private String nullAllowedFlag;

    @Column(name = "UNIQUE_TRANS_REF")
    private String uniqueTransRef;

    @Column(name = "SELECTED")
    private String selected;

    @Column(name = "SOURCE_COLUMN_ID")
    private Long sourceColumnId;

    @Column(name = "MAPPING_TYPE")
    private String mappingType;

    @Column(name = "MAPPING_SET_ID")
    private Long mappingSetId;

    @Column(name = "MAPPING_VALUE1")
    private String mappingValue1;

    @Column(name = "MAPPING_VALUE2")
    private String mappingValue2;

    @Column(name = "MAPPING_VALUE3")
    private String mappingValue3;

    @Column(name = "MAPPING_VALUE4")
    private String mappingValue4;

    @Column(name = "MAPPING_VALUE5")
    private String mappingValue5;

    @Column(name = "DISPLAY_SEQ")
    private Integer seq;
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
