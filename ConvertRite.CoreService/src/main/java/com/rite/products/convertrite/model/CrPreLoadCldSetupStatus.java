package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_PRELOAD_CLD_SETUP_STATUS")
public class CrPreLoadCldSetupStatus {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name = "SETUP_ID")
    private Long setupId;
    @Column(name="PROJECT_ID")
    private Long projectId;
    @Column(name="OBJECT_ID")
    private Long objectId;
    @Column(name="CLD_TEMPLATE_CODE")
    private String cldTemplateCode;
    @Column(name="CLD_TEMPLATE_NAME")
    private String cldTemplateName;
    @Column(name="CLD_METADATA_TABLE_NAME")
    private String cldMetaDataTableName;
    @Column(name="CLD_STAGING_TABLE_NAME")
    private String cldStagingTableName;
    @Column(name="VAL_SYNC_TABLES")
    private String valSyncTables;
    @Column(name="VAL_PKG_EXECUTION")
    private String valPkgExecution;
    @Column(name="CLD_SETUP_STATUS")
    private String cldSetUpStatus;
    @Column(name=" CLD_SETUP_ERROR_MESSAGE")
    private String cldSetUpErrorMessage;
    @Column(name="VAL_PKG_STATUS")
    private String valPkgStatus;
    @Column(name="VAL_PKG_ERROR_MESSAGE")
    private String valPkgErrorMessage;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
}
