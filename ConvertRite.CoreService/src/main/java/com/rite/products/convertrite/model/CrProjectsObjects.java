package com.rite.products.convertrite.model;


import lombok.Data;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_PROJECT_OBJECTS")
@IdClass(ProjectObjectId.class)
public class CrProjectsObjects implements Serializable {

//    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name = "PROJECT_OBJ_LINK_ID")
    public Integer projectObjLinkId;
    @Id
    @Column(name = "PROJECT_ID")
    public Long projectId;
    @Column(name = "PROJECT_CODE")
    public String projectCode;
    @Column(name = "MODULE_NAME")
    public String moduleCode;
    @Id
    @Column(name = "OBJECT_ID")
    public Integer objectId;
    @Column(name = "OBJECT_NAME")
    public String objectName;
    @Column(name = "OBJECT_CODE")
    public String objectCode;
    @Column(name = "PARENT_OBJECT_CODE")
    public String parentObjectCode;
    @Column(name = "CREATION_DATE")
    public Date creationDate;
    @Column(name = "CREATED_BY")
    public String createdBy;
    @Column(name = "LAST_UPDATE_DATE")
    public String lastUpdateDate;
    @Column(name = "LAST_UPDATE_BY")
    public String lastUpdateBy;
    @Column(name = "CONVERSION_TYPE")
    public String conversionType;
}
