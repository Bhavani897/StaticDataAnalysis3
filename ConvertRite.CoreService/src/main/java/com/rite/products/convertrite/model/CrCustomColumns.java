package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_CUSTOM_COLUMNS")
@IdClass(CrCustomColumnId.class)
public class CrCustomColumns {
    @Id
    @Column(name="TABLE_ID")
    private Long tableId;
    @Id
    @Column(name="COLUMN_ID")
    private Integer columnId;
    @Column(name="COLUMN_NAME")
    private String columnName;
    @Column(name="USER_COLUMN_NAME")
    private String userColumnName;
    @Column(name="DESCRIPTION")
    private String description;
    @Column(name="APPLICATION_ID")
    private Integer applicationId;
    @Column(name="COLUMN_SEQUENCE")
    private Integer columnSequence;
    @Column(name="COLUMN_TYPE")
    private String columnType;
    @Column(name="WIDTH")
    private Integer width;
    @Column(name="NULL_ALLOWED_FLAG")
    private String nullAllowedFlag;
    @Column(name="TRANSLATE_FLAG")
    private String translateFlag;
    @Column(name="FLEXFIELD_USAGE_CODE")
    private String flexFieldUsageCode;
    @Column(name="FLEXFIELD_APPLICATION_ID")
    private long flexFieldApplicationId;
    @Column(name="FLEXFIELD_NAME")
    private String flexFieldName;
    @Column(name="FLEX_VALUE_SET_APPLICATION_ID")
    private long flexValueSetApplicationId;
    @Column(name="FLEX_VALUE_SET_ID")
    private long flexValueSetId;
    @Column(name="DEFAULT_VALUE")
    private String defaultValue;
    @Column(name="PRECISION")
    private int precision;
    @Column(name="SCALE")
    private int scale;
    @Column(name="IREP_COMMENTS")
    private String irepComments;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;

}
