package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.Column;

@Data
public class CRPreloadValidationSetupResPo {

    @Column(name = "setup_id")
    private Long setupId;

    @Column(name = "project_id")
    private Long projectId;

    @Column(name = "project_name")
    private String projectName;

    @Column(name = "object_id")
    private Long objectId;

    @Column(name = "object_name")
    private String objectName;

    @Column(name = "VAL_SYNC_TABLES")
    private String valSyncTables;

    @Column(name = "VAL_PKG_EXECUTION")
    private String valPkgExecution;

    @Column(name = "VAL_PKG_STATUS")
    private String valPkgStatus;

    @Column(name = "VAL_PKG_ERROR_MESSAGE")
    private String valPkgErrorMessage;

    public CRPreloadValidationSetupResPo(Long setupId, Long projectId, String projectName, Long objectId, String objectName, String valSyncTables, String valPkgExecution, String valPkgStatus, String valPkgErrorMessage) {
        this.setupId = setupId;
        this.projectId = projectId;
        this.projectName = projectName;
        this.objectId = objectId;
        this.objectName = objectName;
        this.valSyncTables = valSyncTables;
        this.valPkgExecution = valPkgExecution;
        this.valPkgStatus = valPkgStatus;
        this.valPkgErrorMessage = valPkgErrorMessage;
    }
}
