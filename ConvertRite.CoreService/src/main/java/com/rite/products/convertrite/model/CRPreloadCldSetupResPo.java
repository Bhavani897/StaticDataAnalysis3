package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.Column;

@Data
public class CRPreloadCldSetupResPo {
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

    @Column(name = "cld_metadata_table_name")
    private String cldMetadataTableName;

    @Column(name = "cld_template_name")
    private String cldTemplateName;

    @Column(name = "cld_template_code")
    private String cldTemplateCode;

    @Column(name = "cld_staging_table_ame")
    private String cldStagingTableName;

    @Column(name = "cld_setup_status")
    private String cldSetupStatus;

    @Column(name = "cld_setup_error_message")
    private String cldSetupErrorMessage;

    public CRPreloadCldSetupResPo(Long setupId, Long projectId, String projectName, Long objectId, String objectName, String cldMetadataTableName, String cldTemplateName, String cldTemplateCode, String cldStagingTableName, String cldSetupStatus, String cldSetupErrorMessage) {
        this.setupId = setupId;
        this.projectId = projectId;
        this.projectName = projectName;
        this.objectId = objectId;
        this.objectName = objectName;
        this.cldMetadataTableName = cldMetadataTableName;
        this.cldTemplateName = cldTemplateName;
        this.cldTemplateCode = cldTemplateCode;
        this.cldStagingTableName = cldStagingTableName;
        this.cldSetupStatus = cldSetupStatus;
        this.cldSetupErrorMessage = cldSetupErrorMessage;
    }

    @Override
    public String toString() {
        return "CrPreloadCldSetupResPo{" +
                "setupId=" + setupId +
                ", projectId=" + projectId +
                ", projectName='" + projectName + '\'' +
                ", objectId=" + objectId +
                ", objectName='" + objectName + '\'' +
                ", cldMetadataTableName='" + cldMetadataTableName + '\'' +
                ", cldTemplateName='" + cldTemplateName + '\'' +
                ", cldTemplateCode='" + cldTemplateCode + '\'' +
                ", cldStagingTableName='" + cldStagingTableName + '\'' +
                ", cldSetupStatus='" + cldSetupStatus + '\'' +
                ", cldSetupErrorMessage='" + cldSetupErrorMessage + '\'' +
                '}';
    }
}
