package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
@Entity
@Data
@Table(name="CR_FBDI_HDRS_VIEW")
public class CrFbdiView {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "FBDI_TEMPLATE_ID")
    private Long fbdiTemplateId;
    
    @Column(name = "FBDI_TEMPLATE_NAME")
    private String fbdiTemplateName;

    @Column(name = "PROJECT_ID")
    private long projectId;
    
    @Column(name = "PROJECT_NAME")
    private String projectName;
    
    @Column(name = "PARENT_OBJECT_ID")
    private long parentObjectId;
    
    @Column(name = "PARENT_OBJECT_NAME")
    private String parentObjName;
    
    @Column(name = "OBJECT_ID")
    private long objectId;
    
    @Column(name = "OBJECT_NAME")
    private String objectName;
    
    @Column(name = "CLOUD_VERSION")
    private String version;
    
    @Column(name = "SHEET_NAME")
    private String sheetName;
    
    @Column(name = "API")
    private String api;
    
    
}
