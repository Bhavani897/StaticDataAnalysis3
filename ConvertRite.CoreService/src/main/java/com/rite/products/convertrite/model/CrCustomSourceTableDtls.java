package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_CUSTOM_SOURCE_TABLE_DTLS")
public class CrCustomSourceTableDtls {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "CUSTOM_TABLE_ID")
    private Long customTableId;
    @Column(name="PROJECT_ID")
    private Long projectId;
    @Column(name="PARENT_OBJECT_ID")
    private long parentObjectId;
    @Column(name="OBJECT_ID")
    private long objectId;
    @Column(name="CUSTOM_TABLE_NAME")
    private String customTableName;
    @Column(name="METADATA_TABLE_ID")
    private long metadataTableId;
    @Column(name="ATTRIBUTE1")
    private String attribute1;
    @Column(name="ATTRIBUTE2")
    private String attribute2;
    @Column(name="ATTRIBUTE3")
    private String attribute3;
    @Column(name="ATTRIBUTE4")
    private String attribute4;
    @Column(name="ATTRIBUTE5")
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
