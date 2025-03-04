package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.sql.Date;

@Entity
@Data
@Table(name="CR_FBDI_TEMPLATE_COLS")
public class CrFbdiCols {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "FBDI_COLUMN_ID")
    private long fbdiColumnId;
    @Column(name = "FBDI_TEMPLATE_ID")
    private long fbdiTemplateId;
    @Column(name = "FBDI_COLUMN_NAME")
    private String fbdiColumnName;
    @Column(name = "REQUIRED")
    private String required;
    @Column(name = "DATABASE_TABLE")
    private String databaseTable;
    @Column(name = "DATABASE_COLUMN")
    private String databaseColumn;
    @Column(name = "COLUMN_SEQUENCE")
    private Integer sequence;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
}
