package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_CUSTOM_TABLES")
public class CrCustomTables {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "TABLE_ID")
    private Long tableId;
    @Column(name="TABLE_NAME")
    private String tableName;
    @Column(name="USER_TABLE_NAME")
    private String userTableName;
    @Column(name="DESCRIPTION")
    private String description;
    @Column(name="OBJECT_ID")
    private long objectId;
    @Column(name="APPLICATION_ID")
    private Integer applicationId;
    @Column(name="AUTO_SIZE")
    private String autoSize;
    @Column(name="TABLE_TYPE")
    private String tableType;
    @Column(name="INITIAL_EXTENT")
    private Integer initialExtent;
    @Column(name="NEXT_EXTENT")
    private Integer nextExtent;
    @Column(name="MIN_EXTENTS")
    private Integer minExtents;
    @Column(name="MAX_EXTENTS")
    private Integer maxExtents;
    @Column(name="PCT_INCREASE")
    private Integer pctIncrease;
    @Column(name="INI_TRANS")
    private Integer iniTrans;
    @Column(name="MAX_TRANS")
    private Integer maxTrans;
    @Column(name="PCT_FREE")
    private Integer pctFree;
    @Column(name="PCT_USED")
    private Integer pctUsed;
    @Column(name="HOSTED_SUPPORT_STYLE")
    private String hostedSupportStyle;
    @Column(name="IREP_COMMENTS")
    private String irepComments;
    @Column(name="IREP_ANNOTATIONS")
    private String irepAnnotations;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;

}

