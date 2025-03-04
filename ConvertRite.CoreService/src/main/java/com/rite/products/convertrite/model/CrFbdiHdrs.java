package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;
@Entity
@Data
@Table(name="CR_FBDI_TEMPLATE_HDRS")
public class CrFbdiHdrs {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "FBDI_TEMPLATE_ID")
    private long fbdiTemplateId;
    @Column(name = "FBDI_TEMPLATE_NAME")
    private String fbdiTemplateName;

    @Column(name = "PROJECT_ID")
    private long projectId;
    @Column(name = "PARENT_OBJECT_ID")
    private long parentObjectId;
    @Column(name = "OBJECT_ID")
    private long objectId;
    @Column(name = "CLOUD_VERSION")
    private String version;
    @Column(name = "SHEET_NAME")
    private String sheetName;
    @Column(name = "API")
    private String api;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
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
}
